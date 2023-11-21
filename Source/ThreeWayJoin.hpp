#include <emmintrin.h>

#pragma once
#include <immintrin.h>
namespace {
using namespace facebook::velox;
using namespace datagenerator;
using Table = std::vector<std::pair<int64_t, int64_t>>;

inline uint32_t murmur_hash(const void* key, int len, uint32_t seed);
inline void heapify(Table& table);
inline void heapsort(Table& table);
inline void hash_join(Table& large, Table& small, Table& output);
inline void merge_join(Table& left, Table& right, std::vector<int64_t>& col1, std::vector<int64_t>& col2);

class MultiWayJoinNode : public core::PlanNode {
public:
  MultiWayJoinNode(const core::PlanNodeId& id, std::vector<core::PlanNodePtr> sources)
      : PlanNode(id), sources_{sources} {}

  // Output type is the type of the first input
  const RowTypePtr& outputType() const override {
    static auto type =
        ROW({"a", "f"}, {CppToType<int64_t>::create(), CppToType<int64_t>::create()});
    return type;
  }

  const std::vector<core::PlanNodePtr>& sources() const override { return sources_; }

  std::string_view name() const override { return "three way join"; }

private:
  // One can add details about the plan node and its metadata in a textual
  // format.
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

static std::vector<std::vector<std::pair<int64_t, int64_t>>> inputs;
static std::vector<std::pair<std::string, std::string>> inputNames;

// Second, let's define the operator. Here's where the main logic lives.
template <int phase> class MultiWayJoinOperator : public exec::Operator {
public:
  // The operator takes the plan node defined above, which could contain
  // additional metadata.
  MultiWayJoinOperator(int32_t operatorId, exec::DriverCtx* driverCtx,
                       std::shared_ptr<const MultiWayJoinNode> planNode)
      : Operator(driverCtx, nullptr, operatorId, planNode->id(), "DuplicateRow") {}

  // Called every time there's input available. We just save it in the `input_`
  // member defined in the base class, and process it on `getOutput()`.
  void addInput(RowVectorPtr input) override {
    if(phase == 0 && input) {
      auto buffer = input->childAt(0)->asFlatVector<int64_t>();
      auto buffer2 = input->childAt(1)->asFlatVector<int64_t>();
      std::vector<std::pair<int64_t, int64_t>> table;
      for(auto i = 0u; i < buffer->size(); i++) {
        table.emplace_back(buffer->valueAtFast(i), buffer2->valueAtFast(i));
      }
      static std::mutex input2Mutex;
      input2Mutex.lock();
      // keep the names of the input columns
      inputNames.emplace_back(dynamic_pointer_cast<const RowType>(input->type())->names().at(0),
                              dynamic_pointer_cast<const RowType>(input->type())->names().at(1));
      inputs.push_back(std::move(table));
      input2Mutex.unlock();
    }
    input_ = input;
  }

  bool needsInput() const override { return !noMoreInput_; }

  // Called every time your operator needs to produce data. It processes the
  // input saved in `input_` and returns a new RowVector.
  RowVectorPtr getOutput() override {
    if(phase == 0 || input_ == nullptr) {
      return nullptr;
    }
    while(inputs.size() < 2) { // wait input
      __asm("pause");
    }

    // We move `input_` to signal the input has been processed.
    auto currentInput = std::move(input_);

    std::vector<int64_t> firstResultColumn, secondResultColumn;
    auto buffer = currentInput->childAt(0)->template asFlatVector<int64_t>();
    auto buffer2 = currentInput->childAt(1)->template asFlatVector<int64_t>();

    // make sure the inputs are ordered correctly
    auto& input0 = inputNames[0].first == "c" ? inputs[0] : inputs[1];
    auto& input1 = inputNames[0].first != "c" ? inputs[0] : inputs[1];

    //=============================================
    // Store buffer into local variable 
    Table table_ba;
    for(auto i = 0u; i < buffer->size(); i++) {
        table_ba.emplace_back(buffer2->valueAtFast(i), buffer->valueAtFast(i));
    }

    // Perform hash join
    Table table_01;
    table_01.reserve(input0.size() * input1.size());
    hash_join(input0, input1, table_01);

    // Perform HeapSort on table_ba's b
    heapify(table_ba);
    heapsort(table_ba);

    // Perform HeapSort on input0's c
    heapify(table_01);
    heapsort(table_01);

    // Perform merge join
    merge_join(table_ba, table_01, firstResultColumn, secondResultColumn);

    //=============================================

    inputs.clear();
    inputNames.clear();
    if(firstResultColumn.size() == 0)
      return nullptr;
    return makeRowVector({"a", "f"}, {makeFlatVector<int64_t>(firstResultColumn),
                                      makeFlatVector<int64_t>(secondResultColumn)});
  }

  // This simple operator is never blocked.
  exec::BlockingReason isBlocked(ContinueFuture* future) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override { return !needsInput(); }
};

// Third, we need to define a plan translation logic to convert our custom plan
// node into our custom operator. Check `velox/exec/LocalPlanner.cpp` for more
// details.
class MultiWayJoinTranslator : public exec::Operator::PlanNodeTranslator {
  std::unique_ptr<exec::Operator> toOperator(exec::DriverCtx* ctx, int32_t id,
                                             const core::PlanNodePtr& node) override {
    if(auto dupRowNode = std::dynamic_pointer_cast<const MultiWayJoinNode>(node)) {
      return std::make_unique<MultiWayJoinOperator<1>>(id, ctx, dupRowNode);
    }
    return nullptr;
  }

  exec::OperatorSupplier toOperatorSupplier(const core::PlanNodePtr& node) override {
    if(auto dupRowNode = std::dynamic_pointer_cast<const MultiWayJoinNode>(node)) {
      return [dupRowNode](int32_t id, exec::DriverCtx* ctx) {
        return std::make_unique<MultiWayJoinOperator<0>>(id, ctx, dupRowNode);
      };
    }
    return nullptr;
  };
};

inline void hash_join(Table& large, Table& small, Table& output) {
  std::vector<std::optional<std::pair<int64_t, int64_t>>> hashTable;
  unsigned int capacity_pow2 = 1;
  while (capacity_pow2 < small.size() * 4) capacity_pow2 <<= 1;
  hashTable.resize(capacity_pow2);

  for(auto i = 0u; i < small.size(); i++) {
    auto buildInput = small[i];
    auto hashValue = murmur_hash(&buildInput.first, 8, 0) % hashTable.size();
    while (hashTable[hashValue].has_value()) {
      hashValue = ++hashValue % hashTable.size();
    }
    hashTable[hashValue] = buildInput;
  }

  for(auto i = 0u; i < large.size(); i++) {
    auto probeInput = large[i];
    auto hashValue = murmur_hash(&probeInput.second, 8, 0) % hashTable.size();
    while (hashTable[hashValue].has_value()) {
      if (hashTable[hashValue].value().first == probeInput.second) {
        output.emplace_back(probeInput.first, hashTable[hashValue].value().second);
      }
      hashValue = ++hashValue % hashTable.size();
    }
  }

}

inline void merge_join(Table& left, Table& right, std::vector<int64_t>& col1, std::vector<int64_t>& col2) {
    auto leftI = 0;
    auto rightI = 0;
    while (leftI < left.size() && rightI < right.size()) {
      auto leftInput = left[leftI];
      auto rightInput = right[rightI];
      if (leftInput.first < rightInput.first)
        leftI++;
      else if (leftInput.first > rightInput.first)
        rightI++;
      else {
        auto val = leftInput.first;
        int i, j;
        for (i = leftI; i < left.size() && left[i].first == val; i++) {
          for (j = rightI; j < right.size() && right[j].first == val; j++) {
            col1.emplace_back(left[i].second);
            col2.emplace_back(right[j].second);
          }
        }
        leftI = i;
        rightI = j;
      }
    }
}

inline void heapify(Table& table) {
  for (int i = 0u; i < table.size(); i++) {
    if (table[i] > table[(i - 1) / 2]) {
      int j = i;
      while (table[j] > table[(j - 1) / 2]) {
        std::swap(table[j], table[(j - 1) / 2]);
        j = (j - 1) / 2;
      }
    }
  }
};

inline void heapsort(Table& table) {
  for (int i = table.size() - 1; i > 0; i--) {
    std::swap(table[0], table[i]);
    int j = 0, index;
    do {
      index = (2 * j + 1);
      if (index < (i - 1) && table[index] < table[index + 1]) {
          index++;
      }
      if (index < i && table[j] < table[index]) {
        std::swap(table[j], table[index]);
      }
      j = index;
    } while (index < i);
  }
};

inline uint32_t murmur_hash(const void* key, int len, uint32_t seed) {
  const uint32_t m = 0x5bd1e995;
  const int r = 24;
  uint32_t h = seed ^ len;
  const unsigned char* data = (const unsigned char*)key;
  while(len >= 4) {
      uint32_t k = *(uint32_t*)data;
      k *= m;
      k ^= k >> r;
      k *= m;
      h *= m;
      h ^= k;
      data += 4;
      len -= 4;
  }
  switch(len) {
      case 3: h ^= data[2] << 16;
      case 2: h ^= data[1] << 8;
      case 1: h ^= data[0];
          h *= m;
  }
  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;
  return h;
};

} // namespace