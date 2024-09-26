#ifndef DATABASE_OPS_H
#define DATABASE_OPS_H

#include <sqlite3.h>
#include <string>

void initDB();
void setKeyValue(const std::string& key, const std::string& value);
std::string getKeyValue(const std::string& key);

#endif