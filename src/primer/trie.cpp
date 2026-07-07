#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // Walk down the trie following each character of `key`. If at any point the
  // required child is missing, the key is not present and we return nullptr.
  std::shared_ptr<const TrieNode> cur = root_;
  if (cur == nullptr) {
    return nullptr;
  }
  for (char ch : key) {
    auto it = cur->children_.find(ch);
    if (it == cur->children_.end()) {
      return nullptr;
    }
    cur = it->second;
  }
  // The node for the full key must exist and must be a value node. Use
  // dynamic_cast to check that the stored value type matches `T`.
  if (!cur->is_value_node_) {
    return nullptr;
  }
  const auto *value_node = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (value_node == nullptr) {
    return nullptr;
  }
  return value_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  auto shared_value = std::make_shared<T>(std::move(value));

  // We rebuild the path from the root to the target node, cloning every node
  // along the way (copy-on-write), while sharing all untouched subtrees.
  if (key.empty()) {
    // The value lives on the root node itself.
    std::shared_ptr<TrieNodeWithValue<T>> new_root;
    if (root_ == nullptr) {
      new_root = std::make_shared<TrieNodeWithValue<T>>(shared_value);
    } else {
      new_root = std::make_shared<TrieNodeWithValue<T>>(root_->children_, shared_value);
    }
    return Trie(std::move(new_root));
  }

  // Clone the root (or create a fresh one if the trie is empty).
  std::shared_ptr<TrieNode> new_root = (root_ == nullptr) ? std::make_shared<TrieNode>() : root_->Clone();

  std::shared_ptr<TrieNode> parent = new_root;
  for (size_t i = 0; i < key.size(); i++) {
    char ch = key[i];
    bool last = (i + 1 == key.size());
    auto it = parent->children_.find(ch);
    std::shared_ptr<const TrieNode> child = (it == parent->children_.end()) ? nullptr : it->second;

    std::shared_ptr<TrieNode> new_child;
    if (last) {
      // Terminal node: it becomes a value node preserving any existing children.
      if (child == nullptr) {
        new_child = std::make_shared<TrieNodeWithValue<T>>(shared_value);
      } else {
        new_child = std::make_shared<TrieNodeWithValue<T>>(child->children_, shared_value);
      }
    } else {
      // Intermediate node: clone the existing child, or make a new empty node.
      new_child = (child == nullptr) ? std::make_shared<TrieNode>() : child->Clone();
    }
    parent->children_[ch] = new_child;
    parent = new_child;
  }

  return Trie(std::move(new_root));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (root_ == nullptr) {
    return *this;
  }

  // First confirm the key exists; otherwise return the original trie unchanged.
  {
    std::shared_ptr<const TrieNode> cur = root_;
    for (char ch : key) {
      auto it = cur->children_.find(ch);
      if (it == cur->children_.end()) {
        return *this;
      }
      cur = it->second;
    }
    if (!cur->is_value_node_) {
      return *this;
    }
  }

  // Clone the path from the root down to the target node, collecting the cloned
  // nodes so we can prune empty ones on the way back up. `path[0]` is always the
  // (possibly demoted) new root.
  std::vector<std::shared_ptr<TrieNode>> path;
  std::shared_ptr<TrieNode> parent = root_->Clone();
  path.push_back(parent);
  for (char ch : key) {
    auto it = parent->children_.find(ch);
    std::shared_ptr<TrieNode> new_child = it->second->Clone();
    parent->children_[ch] = new_child;
    path.push_back(new_child);
    parent = new_child;
  }

  // The last node in `path` is the target. Convert it back to a plain TrieNode
  // (dropping the value) while keeping its children.
  std::shared_ptr<TrieNode> target = path.back();
  std::shared_ptr<TrieNode> demoted = std::make_shared<TrieNode>(target->children_);
  path.back() = demoted;
  if (path.size() >= 2) {
    path[path.size() - 2]->children_[key[key.size() - 1]] = demoted;
  }

  // Walk back up: any node that has no children and holds no value is pruned
  // from its parent so the trie stays minimal.
  for (size_t idx = path.size() - 1; idx >= 1; idx--) {
    std::shared_ptr<TrieNode> node = path[idx];
    if (node->children_.empty() && !node->is_value_node_) {
      char ch = key[idx - 1];
      path[idx - 1]->children_.erase(ch);
    } else {
      break;
    }
  }

  // `path[0]` is the effective new root (it may have been demoted when key is
  // empty). If it has become empty and valueless, the resulting trie is empty.
  std::shared_ptr<TrieNode> effective_root = path[0];
  if (effective_root->children_.empty() && !effective_root->is_value_node_) {
    return {};
  }
  return Trie(std::move(effective_root));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
