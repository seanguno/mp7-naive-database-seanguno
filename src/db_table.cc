#include "db_table.hpp"

//const unsigned int kRowGrowthRate = 2;

void DbTable::AddColumn(const std::pair<std::string, DataType>& col_desc) {
    if (col_descs_.size() == row_col_capacity_) {
        DbTable::ResizeRow();
    }
    col_descs_.push_back(col_desc);
    for (auto const& [key, value] : rows_) {
        switch (col_descs_.at(col_descs_.size() - 1).second)
        {
        case DataType::kString :
            rows_.at(key)[col_descs_.size() - 1] = static_cast<void*>(new std::string(""));
            break;
        case DataType::kDouble :
            rows_.at(key)[col_descs_.size() - 1] = static_cast<void*>(new double(0.0));
            break;
        case DataType::kInt :
            rows_.at(key)[col_descs_.size() - 1] = static_cast<void*>(new int(0));
            break;
        default:
            break;
        }
    }
}
void DbTable::DeleteColumnByIdx(unsigned int col_idx) {
    if (col_idx < 0 || col_idx > col_descs_.size() - 1) {
        throw std::out_of_range("out of range");
    }
    if (col_descs_.size() == 1 && !rows_.empty()) {
        throw std::runtime_error("any table with at least one row must have at least one column");
    }
    for (auto const& [key, value] : rows_) {
        switch (col_descs_.at(col_idx).second)
        {
        case DataType::kString :
            delete static_cast<std::string*>(rows_.at(key)[col_idx]);
            break;
        case DataType::kDouble :
            delete static_cast<double*>(rows_.at(key)[col_idx]);
            break;
        case DataType::kInt :
            delete static_cast<int*>(rows_.at(key)[col_idx]);
            break;
        default:
            break;
        }
        for (size_t i = col_idx; i < col_descs_.size() - 1; ++i) {
            rows_.at(key)[i] = rows_.at(key)[i + 1];
        }
        rows_.at(key)[col_descs_.size() - 1] = nullptr;
    }
    col_descs_.erase(col_descs_.begin() + col_idx);
}

void DbTable::AddRow(const std::initializer_list<std::string>& col_data) {
    if (col_data.size() != col_descs_.size()) {
        throw std::invalid_argument("invalid arg");
    }
    void** to_add = new void*[col_descs_.size()];
    int index = 0;
    for (const std::string& str : col_data) {
        switch (col_descs_.at(index).second)
        {
        case DataType::kString :
            to_add[index] = static_cast<void*>(new std::string(str));
            break;
        case DataType::kDouble :
            to_add[index] = static_cast<void*>(new double(std::stod(str)));
            break;
        case DataType::kInt :
            to_add[index] = static_cast<void*>(new int(std::stoi(str)));
            break;
        default:
            break;
        }
        index++;
    }
   rows_.insert({next_unique_id_, to_add});
   ++next_unique_id_;
}
void DbTable::DeleteRowById(unsigned int id) {
    if (id >= next_unique_id_) {
        throw std::runtime_error("invalid id");
    }
    for (int i = 0; static_cast<size_t>(i) < col_descs_.size(); ++i) {
        switch (col_descs_.at(i).second)
        {
        case DataType::kString :
            delete static_cast<std::string*>(rows_.at(id)[i]);
            break;
        case DataType::kDouble :
            delete static_cast<double*>(rows_.at(id)[i]);
            break;
        case DataType::kInt :
            delete static_cast<int*>(rows_.at(id)[i]);
            break;
        default:
            break;
        }
    }
    delete[] rows_.at(id);
    rows_.erase(id);
}

DbTable::DbTable(const DbTable& rhs)
: row_col_capacity_(rhs.row_col_capacity_), col_descs_(rhs.col_descs_)
{
    void** to_add = nullptr;
    row_col_capacity_ = rhs.row_col_capacity_;
    col_descs_ = rhs.col_descs_;
    next_unique_id_ = rhs.next_unique_id_;
    for (auto const& [key, value] : rhs.rows_) {
        try {
            to_add = new void*[row_col_capacity_];
            
        } catch (std::bad_alloc&) {
            throw std::invalid_argument("invalid arg");
        }
        for (size_t i = 0; i < col_descs_.size(); ++i) {
            void* value_to_add = nullptr;
            switch (col_descs_.at(i).second)
            {
            case DataType::kString : value_to_add = static_cast<void*>(new std::string(*static_cast<std::string*>(rhs.rows_.at(key)[i])));
                break;
            case DataType::kDouble : value_to_add = static_cast<void*>(new double(*static_cast<double*>(rhs.rows_.at(key)[i])));
                break;
            case DataType::kInt : value_to_add = static_cast<void*>(new int(*static_cast<int*>(rhs.rows_.at(key)[i])));
                break;
            default:
                break;
            }
            to_add[i] = value_to_add;
        }
        rows_.insert({key, to_add});
       
    }
    
}

 /*
        delete[] rows_.at(key);
        rows_.at(key) = to_add;
        for (size_t i = 0; i < col_descs_.size(); ++i) {
            rows_.at(key)[i] = rhs.rows_.at(key)[i];
        }
        */

       /*
    for (auto const& [key, value] : rhs.rows_) {
        try {
            void** to_add = new void*[row_col_capacity_];
            for (size_t i = 0; i < row_col_capacity_; ++i) {
                to_add[i] = rhs.rows_.at(key)[i];
            }
            rows_.insert({next_unique_id_, to_add});
            ++next_unique_id_;
        } catch (std::bad_alloc&) {
            throw std::invalid_argument("invalid arg");
        }
    }
    */
DbTable& DbTable::operator=(const DbTable& rhs) {
    if (rhs.next_unique_id_ > 0 || this == &rhs) return *this;
    void** to_add = nullptr;
    row_col_capacity_ = rhs.row_col_capacity_;
    col_descs_ = rhs.col_descs_;
    next_unique_id_ = rhs.next_unique_id_;
    for (auto &pair : rows_) {
        delete rows_[pair.first];
    }
    rows_.clear();
    for (auto const& [key, value] : rows_) {
        delete[] rows_.at(key);
    }
    for (auto const& [key, value] : rhs.rows_) {
        try {
            to_add = new void*[row_col_capacity_];
            
        } catch (std::bad_alloc&) {
            throw std::invalid_argument("invalid arg");
        }

        for (size_t i = 0; i < col_descs_.size(); ++i) {
            void* value_to_add = nullptr;
            switch (col_descs_.at(i).second)
            {
            case DataType::kString : value_to_add = static_cast<void*>(new std::string(*static_cast<std::string*>(rhs.rows_.at(key)[i])));
                break;
            case DataType::kDouble : value_to_add = static_cast<void*>(new double(*static_cast<double*>(rhs.rows_.at(key)[i])));
                break;
            case DataType::kInt : value_to_add = static_cast<void*>(new int(*static_cast<int*>(rhs.rows_.at(key)[i])));
                break;
            default:
                break;
            }
            to_add[i] = value_to_add;
        }
        rows_.insert({key, to_add});
    }
    return *this;
}
/*
        delete[] rows_.at(key);
        rows_.at(key) = to_add;
        for (size_t i = 0; i < col_descs_.size(); ++i) {
            rows_.at(key)[i] = rhs.rows_.at(key)[i];
        }
        */
DbTable::~DbTable() {
    for (unsigned int i = 0; i < next_unique_id_; ++i) {
        if (rows_.contains(i)) {
            //DbTable::DeleteRowById(i);
            for (int j = 0; static_cast<size_t>(j) < col_descs_.size(); ++j) {
                switch (col_descs_.at(j).second)
                {
                case DataType::kString :
                    delete static_cast<std::string*>(rows_.at(i)[j]);
                    break;
                case DataType::kDouble :
                    delete static_cast<double*>(rows_.at(i)[j]);
                    break;
                case DataType::kInt :
                    delete static_cast<int*>(rows_.at(i)[j]);
                    break;
                default:
                    break;
                }
            }
            delete[] rows_.at(i);
            rows_.erase(i);
        }
    }
}

std::ostream& operator<<(std::ostream& os, const DbTable& table) {
    for (int i = 0; static_cast<size_t>(i) < table.col_descs_.size(); ++i) {
        os << table.col_descs_.at(i).first;
        switch (table.col_descs_.at(i).second)
        {
        case DataType::kString : os << "(std::string)";
            break;
        case DataType::kDouble : os << "(double)";
            break;
        case DataType::kInt : os << "(int)";
            break;
        default:
            break;
        }
        if (static_cast<size_t>(i) < table.col_descs_.size() - 1) {
            os << ", ";
        }
    }
    for (auto const& [key, value] : table.rows_) {
        os << "\n";
        for (int i = 0; static_cast<size_t>(i) < table.col_descs_.size(); ++i) {
            switch (table.col_descs_.at(i).second)
            {
            case DataType::kString : os << *(static_cast<std::string*>(table.rows_.at(key)[i]));
                break;
            case DataType::kDouble : os << *(static_cast<double*>(table.rows_.at(key)[i]));
                break;
            case DataType::kInt : os << *(static_cast<int*>(table.rows_.at(key)[i]));
                break;
            default:
                break;
            }
            if (static_cast<size_t>(i) < table.col_descs_.size() - 1) {
                os << ", ";
            }
        }
    }
    return os;
}



void DbTable::ResizeRow() {
    unsigned int updated_row_col_cap = row_col_capacity_ * 2;
    for (auto const& [key, value] : rows_) {
        void** cpy = new void*[updated_row_col_cap];
        for (size_t i = 0; i < col_descs_.size(); ++i) {
            cpy[i] = rows_.at(key)[i];
        }
        delete[] rows_.at(key);
        rows_.at(key) = cpy;
    }
    row_col_capacity_ = updated_row_col_cap;
}
