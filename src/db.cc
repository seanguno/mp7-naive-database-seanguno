#include "db.hpp"

void Database::CreateTable(const std::string& table_name) {
    tables_.insert({table_name, new DbTable()});
}
void Database::DropTable(const std::string& table_name) {
    if (!(tables_.contains(table_name))) {
        throw std::invalid_argument("table not found");
    }
    (*tables_.at(table_name)).~DbTable();
    tables_.erase(table_name);
}
DbTable& Database::GetTable(const std::string& table_name) {
    return *tables_.at(table_name);
}

Database::Database(const Database& rhs) {
    for (auto const& [key, value] : rhs.tables_) {
        try {
            tables_.insert({key, new DbTable(*rhs.tables_.at(key))});
        } catch (std::bad_alloc&) {
            throw std::invalid_argument("invalid arg");
        }
    }
}
Database& Database::operator=(const Database& rhs) {
    if (this == &rhs) return *this;
    for (auto &pair : tables_) {
        delete tables_[pair.first];
    }
    tables_.clear();
    for (auto const& [key, value] : rhs.tables_) {
        try {
            tables_.insert({key, new DbTable(*rhs.tables_.at(key))});
        } catch (std::bad_alloc&) {
            throw std::invalid_argument("invalid arg");
        }
    }
    return *this;
}
Database::~Database() {
    for (auto &pair : tables_) {
        delete tables_[pair.first];
    }
    tables_.clear();
}
std::ostream& operator<<(std::ostream& os, const Database& db) {
    for (auto const& [key, value] : db.tables_) {
        os << value;
    }
    return os;
}
