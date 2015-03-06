/*
Minetest
Copyright (C) 2015 celeron55, Perttu Ahola <celeron55@gmail.com>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

#include "database-postgresql.h"

#include "settings.h"
#include "log.h"
#include "exceptions.h"


#define PG_RES(res, wanted_res) \
	if (!res || PQresultStatus(res) != (wanted_res)) { \
		throw FileNotGoodException(std::string(\
				"PostgreSQL database error (" \
				__FILE__ ":" TOSTRING(__LINE__) \
				"): ") + \
			(res ? PQresultErrorMessage(res) : "Result is NULL!")); \
	}
#define PG_RES_NOFREE(s, r) { PGresult *macro_res = (s); PG_RES(macro_res, r); }
#define PG_RES_FREE(s, r) { PGresult *macro_res = (s); PG_RES(macro_res, r); PQclear(macro_res); }
#define PG_OK_NOFREE(s) PG_RES_NOFREE(s, PGRES_COMMAND_OK);
#define PG_OK_FREE(s) PG_RES_FREE(s, PGRES_COMMAND_OK);

#define PREPARE_STATEMENT(name, query) \
	PG_OK_FREE(PQprepare(conn, name, query, 0, NULL))


Database_PostgreSQL::Database_PostgreSQL(const Settings &conf)
{
	try {
		conn = PQconnectdb(conf.get("postgresql_connection_info").c_str());
	} catch (SettingNotFoundException) {
		throw SettingNotFoundException("postgresql_connection_info must be "
			"set in world.mt to use the PostgreSQL backend!");
	}

	if (PQstatus(conn) != CONNECTION_OK) {
		throw FileNotGoodException(std::string("Couldn't open "
			"PostgreSQL database connection: ") +
			PQerrorMessage(conn));
	}

	PG_OK_FREE(PQexec(conn,
		// Silence "relation already exists" warning
		"SELECT set_config('client_min_messages', 'error', true);\n"
		"CREATE TABLE IF NOT EXISTS \"blocks\" (\n"
		"	\"x\" INTEGER NOT NULL,\n"
		"	\"y\" INTEGER NOT NULL,\n"
		"	\"z\" INTEGER NOT NULL,\n"
		"	\"data\" BYTEA,\n"
		"	PRIMARY KEY (\"x\", \"y\", \"z\")\n"
		");\n"));

	PREPARE_STATEMENT("begin", "BEGIN");
	PREPARE_STATEMENT("end", "COMMIT");
	PREPARE_STATEMENT("load", "SELECT \"data\" FROM \"blocks\" WHERE "
			"\"x\" = $1 AND \"y\" = $2 AND \"z\" = $3");
	PREPARE_STATEMENT("save", "WITH upsert AS "
			"(UPDATE \"blocks\" SET \"data\"=$4 WHERE "
				"\"x\" = $1 AND \"y\" = $2 AND \"z\" = $3 "
				" RETURNING *) "
			"INSERT INTO \"blocks\" (\"x\", \"y\", \"z\", \"data\") "
				"SELECT $1, $2, $3, $4 "
			"WHERE NOT EXISTS (SELECT * FROM upsert)");
	PREPARE_STATEMENT("delete", "DELETE FROM \"blocks\" WHERE "
			"\"x\" = $1 AND \"y\" = $2 AND \"z\" = $3");
	PREPARE_STATEMENT("list", "SELECT \"x\", \"y\", \"z\" FROM \"blocks\"");

	verbosestream << "ServerMap: PostgreSQL database opened." << std::endl;
}

void Database_PostgreSQL::beginSave() {
	PG_OK_FREE(PQexecPrepared(conn, "begin", 0, NULL, NULL, NULL, 0));
}

void Database_PostgreSQL::endSave() {
	PG_OK_FREE(PQexecPrepared(conn, "end", 0, NULL, NULL, NULL, 0));
}

#define BIND_POS(params, pos) \
	std::string spx = itos(pos.X), \
		spy = itos(pos.Y), \
		spz = itos(pos.Z); \
	params[0] = spx.c_str(); \
	params[1] = spy.c_str(); \
	params[2] = spz.c_str();

bool Database_PostgreSQL::deleteBlock(const v3s16 &pos)
{
	const char *params[3];
	BIND_POS(params, pos);
	PG_OK_FREE(PQexecPrepared(conn, "delete", 3,
		params, NULL, NULL, 0));
	return true;
}

bool Database_PostgreSQL::saveBlock(const v3s16 &pos, const std::string &data)
{
	const char *params[4];
	BIND_POS(params, pos);
	params[3] = data.data();
	const int paramLengths[] = {0, 0, 0, (int) data.size()};
	const int paramFormats[] = {0, 0, 0, 1};
	PG_OK_FREE(PQexecPrepared(conn, "save", 4,
		params, paramLengths, paramFormats, 0));
	return true;
}

std::string Database_PostgreSQL::loadBlock(const v3s16 &pos)
{
	const char *params[3];
	BIND_POS(params, pos);
	PGresult *res = PQexecPrepared(conn, "load", 3,
		params, NULL, NULL, 1);
	PG_RES_NOFREE(res, PGRES_TUPLES_OK);

	std::string data;

	if (PQntuples(res) != 0) {
		data = std::string(PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0));
	}

	PQclear(res);
	return data;
}

void Database_PostgreSQL::listAllLoadableBlocks(std::vector<v3s16> &dst)
{
	PGresult *res = PQexecPrepared(conn, "list", 0,
		NULL, NULL, NULL, 0);
	PG_RES_NOFREE(res, PGRES_TUPLES_OK);

	int rows = PQntuples(res);
	dst.reserve(rows);

	for (int i = 0; i < rows; i++) {
		v3s16 pos;
		pos.X = atoi(PQgetvalue(res, i, 0));
		pos.Y = atoi(PQgetvalue(res, i, 1));
		pos.Z = atoi(PQgetvalue(res, i, 2));
		dst.push_back(pos);
	}

	PQclear(res);
}

Database_PostgreSQL::~Database_PostgreSQL()
{
	PQfinish(conn);
}

