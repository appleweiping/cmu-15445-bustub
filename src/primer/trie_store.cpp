#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // (1) Take a snapshot of the current root under the root lock, then release it
  //     immediately so lookups never block other readers or the single writer.
  Trie snapshot;
  {
    std::lock_guard<std::mutex> guard(root_lock_);
    snapshot = root_;
  }
  // (2) Look up the value against the snapshot (no lock held).
  const T *value = snapshot.Get<T>(key);
  if (value == nullptr) {
    return std::nullopt;
  }
  // (3) Return a ValueGuard that keeps the snapshot root alive so the reference
  //     to the value stays valid even if the store is later mutated.
  return std::make_optional<ValueGuard<T>>(snapshot, *value);
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // Only one writer at a time: hold write_lock_ for the whole read-modify-write.
  std::lock_guard<std::mutex> write_guard(write_lock_);
  Trie snapshot;
  {
    std::lock_guard<std::mutex> guard(root_lock_);
    snapshot = root_;
  }
  // Build the new trie outside the root lock (COW, so this is cheap and safe).
  Trie new_trie = snapshot.Put<T>(key, std::move(value));
  {
    std::lock_guard<std::mutex> guard(root_lock_);
    root_ = std::move(new_trie);
  }
}

void TrieStore::Remove(std::string_view key) {
  std::lock_guard<std::mutex> write_guard(write_lock_);
  Trie snapshot;
  {
    std::lock_guard<std::mutex> guard(root_lock_);
    snapshot = root_;
  }
  Trie new_trie = snapshot.Remove(key);
  {
    std::lock_guard<std::mutex> guard(root_lock_);
    root_ = std::move(new_trie);
  }
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
