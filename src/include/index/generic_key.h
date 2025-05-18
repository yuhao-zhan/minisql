#ifndef MINISQL_GENERIC_KEY_H
#define MINISQL_GENERIC_KEY_H

#include <cstring>

#include "record/field.h"
#include "record/row.h"

class GenericKey {
  friend class KeyManager;
  char data[0];
};

class KeyManager {
 public: /**/
  [[nodiscard]] inline GenericKey *InitKey() const {
    return (GenericKey *)malloc(key_size_);  // remember delete
  }

  inline void SerializeFromKey(GenericKey *key_buf, const Row &key, Schema *schema) const {
    // initialize to 0
    [[maybe_unused]] uint32_t size = key.GetSerializedSize(key_schema_);
    ASSERT(key.GetFieldCount() == schema->GetColumnCount(), "field nums not match.");
    // std::cout << "size " << size << " key size " << key_size_ << std::endl;
    ASSERT(size <= (uint32_t)key_size_, "Index key size exceed max key size.");
    memset(key_buf->data, 0, key_size_);
    key.SerializeTo(key_buf->data, schema);
  }

  inline void DeserializeToKey(const GenericKey *key_buf, Row &key, Schema *schema) const {
      std::cout << "key_buf: " << (void *)key_buf << std::endl;
    [[maybe_unused]] uint32_t ofs = key.DeserializeFrom(const_cast<char *>(key_buf->data), schema);
    ASSERT(ofs <= (uint32_t)key_size_, "Index key size exceed max key size.");
  }

  // compare
  [[nodiscard]] inline int CompareKeys(const GenericKey *lhs, const GenericKey *rhs) const {
      std::cout << "lhs: " << (void *)lhs << " rhs: " << (void *)rhs << std::endl;
    //    ASSERT(malloc_usable_size((void *)&lhs) == malloc_usable_size((void *)&rhs), "key size not match.");
    uint32_t column_count = key_schema_->GetColumnCount();
    Row rhs_key(INVALID_ROWID);
    Row lhs_key(INVALID_ROWID);


    DeserializeToKey(rhs, rhs_key, key_schema_);
    DeserializeToKey(lhs, lhs_key, key_schema_);

    std::cout << "Deserialization finish!" << std::endl;
    for (uint32_t i = 0; i < column_count; i++) {
      Field *lhs_value = lhs_key.GetField(i);
      Field *rhs_value = rhs_key.GetField(i);

      if (lhs_value->CompareLessThan(*rhs_value) == CmpBool::kTrue) {
        return -1;
      }

      if (lhs_value->CompareGreaterThan(*rhs_value) == CmpBool::kTrue) {
        return 1;
      }
    }
    // equals
    return 0;
  }

  inline int GetKeySize() const { return key_size_; }

  KeyManager(const KeyManager &other) {
    this->key_schema_ = other.key_schema_;
    this->key_size_ = other.key_size_;
  }

  // constructor
  KeyManager(Schema *key_schema, size_t key_size) :  key_schema_(key_schema) {
      // ---- 1. 计算 header 开销 ----
      uint32_t col_count   = key_schema_->GetColumnCount();
      uint32_t bitmap_bytes = (col_count + 7) / 8;
      // RowId(2×4 bytes) + col_count(4 bytes) + bitmap
      size_t required = sizeof(uint32_t) * 2 + sizeof(uint32_t) + bitmap_bytes;

      // ---- 2. 加上每列的最大数据长度 ----
      for (uint32_t i = 0; i < col_count; i++) {
          const Column *col = key_schema_->GetColumn(i);
          switch (col->GetType()) {
              case TypeId::kTypeInt:
                  required += sizeof(int32_t);
                  break;
              case TypeId::kTypeFloat:
                  required += sizeof(float);
                  break;
              case TypeId::kTypeChar:
                  // 假设 Column 有方法 GetLength() 返回最大字符数
                  required += col->GetLength();
                  break;
              default:
                  // 若有其它类型，按实际补充
                  required += 8;  // 保守假设 8 字节
          }
      }

      // ---- 3. 最终 key_size_ ----
      key_size_ = std::max<size_t>(key_size, required);
  }

 private:
  int key_size_;
  Schema *key_schema_;
};

#endif  // MINISQL_GENERIC_KEY_H
