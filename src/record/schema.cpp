#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
    // 1. 写入魔数
    char *pos = buf;
    MACH_WRITE_UINT32(pos, Schema::SCHEMA_MAGIC_NUM);
    pos += sizeof(uint32_t);
    // 2. 写入列数
    uint32_t col_count = columns_.size();
    MACH_WRITE_UINT32(pos, col_count);
    pos += sizeof(uint32_t);
    // 3. 写入每个列的序列化数据
    for (const auto &col : columns_) {
      uint32_t sz = col->SerializeTo(pos);
      pos += sz;
    }
    // 4. 写入 is_manage_ 标志
    *pos = static_cast<char>(is_manage_);
    pos += sizeof(char);
    // 5. 返回序列化的字节数
    return pos - buf;
}

uint32_t Schema::GetSerializedSize() const {
    // 1. 魔数 + 列数
    uint32_t size = sizeof(uint32_t) + sizeof(uint32_t);
    // 2. 每个列的序列化大小
    for (const auto &col : columns_) {
      size += col->GetSerializedSize();
    }
    // 3. is_manage_ 标志
    size += sizeof(char);
    return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    // 1. 读魔数
    char *pos = buf;
    uint32_t magic_num = MACH_READ_UINT32(pos);
    pos += sizeof(uint32_t);
    ASSERT(magic_num == Schema::SCHEMA_MAGIC_NUM, "Schema DeserializeFrom wrong magic number.");
    // 2. 读列数
    uint32_t col_count = MACH_READ_UINT32(pos);
    pos += sizeof(uint32_t);
    // 3. 创建列数组并反序列化每个列
    std::vector<Column *> columns;
    for (uint32_t i = 0; i < col_count; ++i) {
        Column *col = nullptr;
        uint32_t sz = Column::DeserializeFrom(pos, col);
        columns.push_back(col);
        pos += sz;
    }
    // 4. 创建 Schema 对象并设置 is_manage_ 标志
    bool is_manage = static_cast<bool>(*pos);
    schema = new Schema(columns, is_manage);
    return pos - buf + sizeof(char); // 返回总字节数
}