#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    char *pos = buf;
    // 1. 写入 RowId（page_id + slot_num 各 4 字节）
    std::cout << "RowId: " << rid_.GetPageId() << ", " << rid_.GetSlotNum() << std::endl;
    MACH_WRITE_UINT32(pos, rid_.GetPageId());
    pos += sizeof(uint32_t);
    MACH_WRITE_UINT32(pos, rid_.GetSlotNum());
    pos += sizeof(uint32_t);

    // 2. 写入列数
    uint32_t col_count = schema->GetColumnCount();
    MACH_WRITE_UINT32(pos, col_count);
    pos += sizeof(uint32_t);

    // 3. 写入 null bitmap
    uint32_t bitmap_bytes = (col_count + 7) / 8;
    // 先清零
    std::memset(pos, 0, bitmap_bytes);
    // 对应位为 1 表示该字段为 NULL
    for (uint32_t i = 0; i < col_count; i++) {
      if (fields_[i]->IsNull()) {
        // 把无符号整型常量 1 左移到第(i%8)位上，得到一个只有该位为1、其它位全为 0 的整数掩码
        pos[i / 8] |= static_cast<char>(1u << (i % 8));
      }
    }
    pos += bitmap_bytes;

    // 4. 写入每个非 NULL 字段的数据
    for (uint32_t i = 0; i < col_count; i++) {
      if (!fields_[i]->IsNull()) {
        uint32_t sz = fields_[i]->SerializeTo(pos);
        pos += sz;
      }
    }

    return pos - buf;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(fields_.empty(), "Non empty field in row.");
    char *pos = buf;
    // 1. 读 RowId
    uint32_t page_id = MACH_READ_UINT32(pos);
    pos += sizeof(uint32_t);
    uint32_t slot_num = MACH_READ_UINT32(pos);
    pos += sizeof(uint32_t);
    rid_.Set(page_id, slot_num);

    // 2. 读列数并校验
    uint32_t col_count = MACH_READ_UINT32(pos);
    pos += sizeof(uint32_t);
    ASSERT(col_count == schema->GetColumnCount(), "Column count mismatch in row deserialize.");

    // 3. 读 null bitmap
    uint32_t bitmap_bytes = (col_count + 7) / 8;
    std::vector<bool> is_null(col_count);
    for (uint32_t i = 0; i < bitmap_bytes; i++) {
      unsigned char byte = *reinterpret_cast<unsigned char *>(pos++);
      for (uint32_t b = 0; b < 8; b++) {
        // 计算当前位对应的列索引
        uint32_t idx = i * 8 + b;
        if (idx < col_count) {
          // 如果当前位对应的列索引小于总列数，则判断该位是否为 NULL
          is_null[idx] = ((byte >> b) & 1u) != 0;
        }
      }
    }

    // 4. 逐字段反序列化
    for (uint32_t i = 0; i < col_count; i++) {
      const Column *col = schema->GetColumn(i);
      Field *field_ptr = nullptr;
      // Field::DeserializeFrom 会根据 is_null 构造 NULL field 或正常读取
      uint32_t consumed = Field::DeserializeFrom(pos, col->GetType(), &field_ptr, is_null[i]);
      pos += consumed;
      fields_.push_back(field_ptr);
    }

    return pos - buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    uint32_t size = 0;
    // RowId 大小
    size += sizeof(uint32_t) * 2;
    // 列数字段
    size += sizeof(uint32_t);
    // null bitmap
    uint32_t col_count = schema->GetColumnCount();
    size += (col_count + 7) / 8;
    // 每个非 NULL 字段自身序列化长度
    for (uint32_t i = 0; i < col_count; i++) {
      if (!fields_[i]->IsNull()) {
        size += fields_[i]->GetSerializedSize();
      }
    }
    return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
