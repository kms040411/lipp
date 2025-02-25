#ifndef __LIPP_H__
#define __LIPP_H__

#include "lipp_base_string.h"
#include <stdint.h>
#include <math.h>
#include <limits>
#include <cstdio>
#include <stack>
#include <vector>
#include <cstring>
#include <sstream>
#include <iostream>
#include <tuple>

// typedef uint8_t bitmap_t;
typedef uint16_t bitmap_t;
#define BITMAP_ZERO 0
#define BITMAP_ONE 0xFFFF
#define BITMAP_WIDTH (sizeof(bitmap_t) * 8)
#define BITMAP_SIZE(num_items) (((num_items) + BITMAP_WIDTH - 1) / BITMAP_WIDTH)
#define BITMAP_GET(bitmap, pos) (((bitmap)[(pos) / BITMAP_WIDTH] >> ((pos) % BITMAP_WIDTH)) & 1)
#define BITMAP_SET(bitmap, pos) ((bitmap)[(pos) / BITMAP_WIDTH] |= 1 << ((pos) % BITMAP_WIDTH))
#define BITMAP_CLEAR(bitmap, pos) ((bitmap)[(pos) / BITMAP_WIDTH] &= ~bitmap_t(1 << ((pos) % BITMAP_WIDTH)))
#define BITMAP_NEXT_1(bitmap_item) __builtin_ctz((bitmap_item))

// runtime assert
#define RT_ASSERT(expr) \
{ \
    if (!(expr)) { \
        fprintf(stderr, "RT_ASSERT Error at %s:%d, `%s`\n", __FILE__, __LINE__, #expr); \
        exit(0); \
    } \
}

#define COLLECT_TIME 0

#if COLLECT_TIME
#include <chrono>
#endif

template<class T, class P, int LEN>
class LIPP
{
    inline int compute_gap_count(int size) {
        return 5;
    }

    struct Node;
    inline int PREDICT_POS(Node* node, T key) const {
        double v = node->model.predict_double(key);
        if (v < 0) {
            return 0;
        }
        if (v > std::numeric_limits<int>::max() / 2) {
            return node->num_items - 1;
        }
        return std::min(node->num_items - 1, static_cast<int>(v));
    }
    const bool QUIET;

    struct {
        #if COLLECT_TIME
        double time_scan_and_destory_tree = 0;
        double time_build_tree_bulk = 0;
        #endif
    } stats;

public:
    typedef std::pair<T, P> V;

    LIPP(bool QUIET = true)
        : QUIET(QUIET) {
        {
            // std::vector<Node*> nodes;
            // char first_key[LEN+1];
            // char second_key[LEN+1];
            // for (int _ = 0; _ < 1e7; _ ++) {
            //     for (size_t idx=0; idx<LEN; idx++) {
            //         first_key[idx] = 32;
            //         second_key[idx] = 126;
            //     }
            //     first_key[LEN] = 0;
            //     second_key[LEN] = 0;

            //     Node* node = build_tree_two(T(first_key), P(), T(second_key), P());
            //     nodes.push_back(node);
            // }
            // for (auto node : nodes) {
            //     destroy_tree(node);
            // }
            // if (!QUIET) {
            //     printf("initial memory pool size = %lu\n", pending_two.size());
            // }
        }
        root = build_tree_none();
    }
    ~LIPP() {
        destroy_tree(root);
        root = NULL;
        destory_pending();
    }

    void insert(const V& v) {
        insert(v.first, v.second);
    }
    void insert(const T& key, const P& value) {
        root = insert_tree(root, key, value);
    }
    P at(const T& key, bool skip_existence_check = true) const {
        Node* node = root;

        while (true) {
            int pos = PREDICT_POS(node, key);
            if (BITMAP_GET(node->child_bitmap, pos) == 1) {
                node = node->items[pos].child;
            } else {
                if (skip_existence_check) {
                    return node->items[pos].value;
                } else {
                    if (BITMAP_GET(node->none_bitmap, pos) == 1) {
                        return -1;
                    } else if (BITMAP_GET(node->child_bitmap, pos) == 0) {
                        RT_ASSERT(node->items[pos].key == key);
                        return node->items[pos].value;
                    }
                }
            }
        }
    }
    bool exists(const T& key) const {
        Node* node = root;
        while (true) {
            int pos = PREDICT_POS(node, key);
            if (BITMAP_GET(node->none_bitmap, pos) == 1) {
                return false;
            } else if (BITMAP_GET(node->child_bitmap, pos) == 0) {
                return node->items[pos].key == key;
            } else {
                node = node->items[pos].child;
            }
        }
    }
    void bulk_load(const V* vs, int num_keys) {
        if (num_keys == 0) {
            destroy_tree(root);
            root = build_tree_none();
            return;
        }
        if (num_keys == 1) {
            destroy_tree(root);
            root = build_tree_none();
            insert(vs[0]);
            return;
        }
        if (num_keys == 2) {
            destroy_tree(root);
            root = build_tree_two(vs[0].first, vs[0].second, vs[1].first, vs[1].second);
            return;
        }

        RT_ASSERT(num_keys > 2);
        std::vector<int> dup_keys;
        for (int i = 1; i < num_keys; i ++) {
            if (!(vs[i].first > vs[i-1].first))
                dup_keys.push_back(i);
            //RT_ASSERT(vs[i].first > vs[i-1].first);
        }

        T* keys = new T[num_keys];
        P* values = new P[num_keys];
        int counter = 0;
        for (int i = 0; i < num_keys; i ++) {
            if ( std::find(dup_keys.begin(), dup_keys.end(), i) == dup_keys.end() ) {
                keys[counter] = vs[i].first;
                values[counter] = vs[i].second;
                counter++;
            }
        }
        destroy_tree(root);
        std::cout << "total keys: " << counter << std::endl;
        root = build_tree_bulk(keys, values, counter);
        delete[] keys;
        delete[] values;
    }
    // Find the keys which are in range [lower, upper], returns the number of found keys.
    int range_query(T* results, const T& lower, const T& upper) const {
        return range_core<false, false>(results, 0, root, lower, upper);
    }
    // Find the minimum `len` keys which are no less than `lower`, returns the number of found keys.
    int range_query_len(T* results, const T& lower, int len) {
        return range_core_len<false>(results, 0, root, lower, len);
    }

    void verify() const {
        std::stack<Node*> s;
        s.push(root);

        while (!s.empty()) {
            Node* node = s.top(); s.pop();
            int sum_size = 0;
            for (int i = 0; i < node->num_items; i ++) {
                if (BITMAP_GET(node->child_bitmap, i) == 1) {
                    s.push(node->items[i].child);
                    sum_size += node->items[i].child->size;
                } else if (BITMAP_GET(node->none_bitmap, i) != 1) {
                    sum_size ++;
                }
            }
            RT_ASSERT(sum_size == node->size);
        }
    }

    size_t index_size(bool total=false, bool ignore_child=true) const {
        std::stack<Node*> s;
        s.push(root);
    
        size_t size = 0;
        while (!s.empty()) {
            Node* node = s.top(); s.pop();
            bool has_child = false;
            if(ignore_child == false) {
                size += sizeof(*node);
            }
            for (int i = 0; i < node->num_items; i ++) {
                if (ignore_child == true) {
                    size += sizeof(Item);
                    has_child = true;
                } else {
                    if (total) size += sizeof(Item);
                }
                if (BITMAP_GET(node->child_bitmap, i) == 1) {
                    if (!total) size += sizeof(Item);
                    s.push(node->items[i].child);
                }
            }
            if (ignore_child == true && has_child) {
                size += sizeof(*node);
            }
        }
        return size;
    }

private:
    struct Node;
    class Item {
        public:
            std::string key;
            P value;
            Node* child;
            Item() { key = ""; value = 0; child=nullptr; }
    };
    class Node
    {
        public:
            int is_two; // is special node for only two keys
            int build_size; // tree size (include sub nodes) when node created
            int size; // current tree size (include sub nodes)
            int fixed; // fixed node will not trigger rebuild
            int num_inserts, num_insert_to_data;
            int num_items; // size of items
            LinearModel<T, LEN> model;
            Item* items;
            bitmap_t* none_bitmap; // 1 means None, 0 means Data or Child
            bitmap_t* child_bitmap; // 1 means Child. will always be 0 when none_bitmap is 1
    };

    Node* root;
    std::stack<Node*> pending_two;

    std::allocator<Node> node_allocator;
    Node* new_nodes(int n)
    {
        Node* p = node_allocator.allocate(n);
        RT_ASSERT(p != NULL && p != (Node*)(-1));
        return p;
    }
    void delete_nodes(Node* p, int n)
    {
        node_allocator.deallocate(p, n);
    }

    //std::allocator<Item> item_allocator;
    Item* new_items(int n)
    {
        Item* p = new Item[n]; //item_allocator.allocate(n);
        RT_ASSERT(p != NULL && p != (Item*)(-1));
        return p;
    }
    void delete_items(Item* p, int n)
    {
        delete[] p;
        //item_allocator.deallocate(p, n);
    }

    std::allocator<bitmap_t> bitmap_allocator;
    bitmap_t* new_bitmap(int n)
    {
        bitmap_t* p = bitmap_allocator.allocate(n);
        RT_ASSERT(p != NULL && p != (bitmap_t*)(-1));
        return p;
    }
    void delete_bitmap(bitmap_t* p, int n)
    {
        bitmap_allocator.deallocate(p, n);
    }

    /// build an empty tree
    Node* build_tree_none()
    {
        Node* node = new_nodes(1);
        node->is_two = 0;
        node->build_size = 0;
        node->size = 0;
        node->fixed = 0;
        node->num_inserts = node->num_insert_to_data = 0;
        node->num_items = 1;
        node->items = new_items(1);
        node->none_bitmap = new_bitmap(1);
        node->none_bitmap[0] = BITMAP_ONE;
        node->child_bitmap = new_bitmap(1);
        node->child_bitmap[0] = BITMAP_ZERO;

        return node;
    }
    /// build a tree with two keys
    Node* build_tree_two(T key1, P value1, T key2, P value2)
    {
        if (key1 > key2) {
            std::swap(key1, key2);
            std::swap(value1, value2);
        }
        RT_ASSERT(key1 < key2);
        // static_assert(BITMAP_WIDTH == 8);

        Node* node = NULL;
        if (pending_two.empty()) {
            node = new_nodes(1);
            node->is_two = 1;
            node->build_size = 2;
            node->size = 2;
            node->fixed = 0;
            node->num_inserts = node->num_insert_to_data = 0;

            node->num_items = 8;
            node->items = new_items(node->num_items);
            node->none_bitmap = new_bitmap(1);
            node->child_bitmap = new_bitmap(1);
            node->none_bitmap[0] = BITMAP_ONE;
            node->child_bitmap[0] = BITMAP_ZERO;
        } else {
            node = pending_two.top(); pending_two.pop();
        }

        const double mid1_target = node->num_items / 3;
        const double mid2_target = node->num_items * 2 / 3;

        std::vector<std::pair<T, double>> train_data;
        train_data.push_back(std::make_pair(key1, mid1_target));
        train_data.push_back(std::make_pair(key2, mid2_target));
        node->model.train(train_data);

        { // insert key1&value1
            int pos = PREDICT_POS(node, key1);
            //RT_ASSERT(BITMAP_GET(node->none_bitmap, pos) == 1);
            BITMAP_CLEAR(node->none_bitmap, pos);
            node->items[pos].key = key1;
            node->items[pos].value = value1;
        }
        { // insert key2&value2
            int pos = PREDICT_POS(node, key2);
            //RT_ASSERT(BITMAP_GET(node->none_bitmap, pos) == 1);
            BITMAP_CLEAR(node->none_bitmap, pos);
            node->items[pos].key = key2;
            node->items[pos].value = value2;
        }

        return node;
    }
    /// bulk build, _keys must be sorted in asc order.
    Node* build_tree_bulk(T* _keys, P* _values, int _size)
    {
        RT_ASSERT(_size > 1);

        typedef struct {
            int begin;
            int end;
            int level; // top level = 1
            Node* node;
        } Segment;
        std::stack<Segment> s;

        Node* ret = new_nodes(1);
        s.push((Segment){0, _size, 1, ret});

        std::vector<std::pair<T, double>> train_data;

        while (!s.empty()) {
            const int begin = s.top().begin;
            const int end = s.top().end;
            const int level = s.top().level;
            Node* node = s.top().node;
            s.pop();

            RT_ASSERT(end - begin >= 2);
            if (end - begin == 2) {
                Node* _ = build_tree_two(_keys[begin], _values[begin], _keys[begin+1], _values[begin+1]);
                memcpy(node, _, sizeof(Node));
                delete_nodes(_, 1);
            } else {
                T* keys = _keys + begin;
                P* values = _values + begin;
                const int size = end - begin;
                const int BUILD_GAP_CNT = compute_gap_count(size);

                node->is_two = 0;
                node->build_size = size;
                node->size = size;
                node->fixed = 0;
                node->num_inserts = node->num_insert_to_data = 0;

                for (int idx=0; idx<size; idx++) {
                    train_data.push_back(std::make_pair(keys[idx], (double)(idx * (BUILD_GAP_CNT + 1) + (BUILD_GAP_CNT + 1) / 2)));
                }
                node->model.train(train_data);
                node->num_items = size * static_cast<int>(BUILD_GAP_CNT + 1);
                train_data.clear();

                // std::vector<std::tuple<T, P, int>> trained_data;
                // for (int idx=0; idx<size; idx++){
                //     trained_data.push_back(std::make_tuple(keys[idx], values[idx], PREDICT_POS(node, keys[idx])));
                // }
                // std::sort(trained_data.begin(), trained_data.end(), [](const auto& a, const auto&b) { return std::get<2>(a) < std::get<2>(b); });
                // for (int idx=0; idx<size; idx++){
                //     keys[idx] = std::get<0>(trained_data[idx]);
                //     values[idx] = std::get<1>(trained_data[idx]);
                // }

                if (size > 1e6) {
                    node->fixed = 1;
                }

                node->items = new_items(node->num_items);
                const int bitmap_size = BITMAP_SIZE(node->num_items);
                node->none_bitmap = new_bitmap(bitmap_size);
                node->child_bitmap = new_bitmap(bitmap_size);
                memset(node->none_bitmap, 0xff, sizeof(bitmap_t) * bitmap_size);
                memset(node->child_bitmap, 0, sizeof(bitmap_t) * bitmap_size);

                for (int item_i = PREDICT_POS(node, keys[0]), offset = 0; offset < size; ) {
                    int next = offset + 1, next_i = -1;
                    while (next < size) {
                        next_i = PREDICT_POS(node, keys[next]);
                        if (next_i == item_i) {
                            next ++;
                        } else {
                            break;
                        }
                    }
                    if (next == offset + 1) {
                        BITMAP_CLEAR(node->none_bitmap, item_i);
                        node->items[item_i].key = keys[offset];
                        node->items[item_i].value = values[offset];
                    } else {
                        // ASSERT(next - offset <= (size+2) / 3);
                        BITMAP_CLEAR(node->none_bitmap, item_i);
                        BITMAP_SET(node->child_bitmap, item_i);
                        node->items[item_i].child = new_nodes(1);
                        s.push((Segment){begin + offset, begin + next, level + 1, node->items[item_i].child});
                    }
                    if (next >= size) {
                        break;
                    } else {
                        item_i = next_i;
                        offset = next;
                    }
                }
            }
        }

        return ret;
    }

    void destory_pending()
    {
        while (!pending_two.empty()) {
            Node* node = pending_two.top(); pending_two.pop();

            delete_items(node->items, node->num_items);
            const int bitmap_size = BITMAP_SIZE(node->num_items);
            delete_bitmap(node->none_bitmap, bitmap_size);
            delete_bitmap(node->child_bitmap, bitmap_size);
            delete_nodes(node, 1);
        }
    }

    void destroy_tree(Node* root)
    {
        std::stack<Node*> s;
        s.push(root);
        while (!s.empty()) {
            Node* node = s.top(); s.pop();

            for (int i = 0; i < node->num_items; i ++) {
                if (BITMAP_GET(node->child_bitmap, i) == 1) {
                    s.push(node->items[i].child);
                }
            }

            if (node->is_two) {
                RT_ASSERT(node->build_size == 2);
                RT_ASSERT(node->num_items == 8);
                node->size = 2;
                node->num_inserts = node->num_insert_to_data = 0;
                node->none_bitmap[0] = BITMAP_ONE;
                node->child_bitmap[0] = BITMAP_ZERO;
                pending_two.push(node);
            } else {
                delete_items(node->items, node->num_items);
                const int bitmap_size = BITMAP_SIZE(node->num_items);
                delete_bitmap(node->none_bitmap, bitmap_size);
                delete_bitmap(node->child_bitmap, bitmap_size);
                delete_nodes(node, 1);
            }
        }
    }

    void scan_and_destory_tree(Node* _root, T* keys, P* values, bool destory = true)
    {
        typedef std::pair<int, Node*> Segment; // <begin, Node*>
        std::stack<Segment> s;

        s.push(Segment(0, _root));
        while (!s.empty()) {
            int begin = s.top().first;
            Node* node = s.top().second;
            const int SHOULD_END_POS = begin + node->size;
            s.pop();

            for (int i = 0; i < node->num_items; i ++) {
                if (BITMAP_GET(node->none_bitmap, i) == 0) {
                    if (BITMAP_GET(node->child_bitmap, i) == 0) {
                        keys[begin] = node->items[i].key;
                        values[begin] = node->items[i].value;
                        begin ++;
                    } else {
                        s.push(Segment(begin, node->items[i].child));
                        begin += node->items[i].child->size;
                    }
                }
            }
            RT_ASSERT(SHOULD_END_POS == begin);

            if (destory) {
                if (node->is_two) {
                    RT_ASSERT(node->build_size == 2);
                    RT_ASSERT(node->num_items == 8);
                    node->size = 2;
                    node->num_inserts = node->num_insert_to_data = 0;
                    node->none_bitmap[0] = BITMAP_ONE;
                    node->child_bitmap[0] = BITMAP_ZERO;
                    pending_two.push(node);
                } else {
                    delete_items(node->items, node->num_items);
                    const int bitmap_size = BITMAP_SIZE(node->num_items);
                    delete_bitmap(node->none_bitmap, bitmap_size);
                    delete_bitmap(node->child_bitmap, bitmap_size);
                    delete_nodes(node, 1);
                }
            }
        }
    }

    Node* insert_tree(Node* _node, const T& key, const P& value)
    {
        constexpr int MAX_DEPTH = 256;
        Node* path[MAX_DEPTH];
        int path_size = 0;
        int insert_to_data = 0;

        for (Node* node = _node; ; ) {
            RT_ASSERT(path_size < MAX_DEPTH);
            path[path_size ++] = node;

            node->size ++;
            node->num_inserts ++;
            int pos = PREDICT_POS(node, key);
            if (BITMAP_GET(node->none_bitmap, pos) == 1) {
                BITMAP_CLEAR(node->none_bitmap, pos);
                node->items[pos].key = key;
                node->items[pos].value = value;
                break;
            } else if (BITMAP_GET(node->child_bitmap, pos) == 0) {
                BITMAP_SET(node->child_bitmap, pos);
                node->items[pos].child = build_tree_two(key, value, node->items[pos].key, node->items[pos].value);
                insert_to_data = 1;
                break;
            } else {
                node = node->items[pos].child;
            }
        }
        for (int i = 0; i < path_size; i ++) {
            path[i]->num_insert_to_data += insert_to_data;
        }

        for (int i = 0; i < path_size; i ++) {
            Node* node = path[i];
            const int num_inserts = node->num_inserts;
            const int num_insert_to_data = node->num_insert_to_data;
            const bool need_rebuild = node->fixed == 0 && node->size >= node->build_size * 4 && node->size >= 64 && num_insert_to_data * 10 >= num_inserts;
            // bool a = node->fixed == 0;
            // bool b = node->size >= node->build_size * 2;
            // bool c = node->size >= 16;
            // bool d = num_insert_to_data * 2 >= num_inserts;
            // //printf("%d / %d %d %d %d\n", node->size, a, b, c, d);
            // const bool need_rebuild = a && b && c && d;
            if (need_rebuild) {
                const int ESIZE = node->size;
                T* keys = new T[ESIZE];
                P* values = new P[ESIZE];

                #if COLLECT_TIME
                auto start_time_scan = std::chrono::high_resolution_clock::now();
                #endif
                scan_and_destory_tree(node, keys, values);
                #if COLLECT_TIME
                auto end_time_scan = std::chrono::high_resolution_clock::now();
                auto duration_scan = end_time_scan - start_time_scan;
                stats.time_scan_and_destory_tree += std::chrono::duration_cast<std::chrono::nanoseconds>(duration_scan).count() * 1e-9;
                #endif

                #if COLLECT_TIME
                auto start_time_build = std::chrono::high_resolution_clock::now();
                #endif
                Node* new_node = build_tree_bulk(keys, values, ESIZE);
                #if COLLECT_TIME
                auto end_time_build = std::chrono::high_resolution_clock::now();
                auto duration_build = end_time_build - start_time_build;
                stats.time_build_tree_bulk += std::chrono::duration_cast<std::chrono::nanoseconds>(duration_build).count() * 1e-9;
                #endif

                delete[] keys;
                delete[] values;

                path[i] = new_node;
                if (i > 0) {
                    int pos = PREDICT_POS(path[i-1], key);
                    path[i-1]->items[pos].child = new_node;
                }

                break;
            }
        }

        return path[0];
    }

    // SATISFY_LOWER = true means all the keys in the subtree of `node` are no less than to `lower`.
    // SATISFY_UPPER = true means all the keys in the subtree of `node` are no greater than to `upper`.
    template<bool SATISFY_LOWER, bool SATISFY_UPPER>
    int range_core(T* results, int pos, Node* node, const T& lower, const T& upper) const
    {
        if constexpr (SATISFY_LOWER && SATISFY_UPPER) {
            int bit_pos = 0;
            const bitmap_t* none_bitmap = node->none_bitmap;
            while (bit_pos < node->num_items) {
                bitmap_t not_none = ~(*none_bitmap);
                while (not_none) {
                    int latest_pos = BITMAP_NEXT_1(not_none);
                    not_none ^= 1 << latest_pos;

                    int i = bit_pos + latest_pos;
                    if (BITMAP_GET(node->child_bitmap, i) == 0) {
                        results[pos] = node->items[i].key;
                        // __builtin_prefetch((void*)&(node->items[i].key) + 64);
                        pos ++;
                    } else {
                        pos = range_core<true, true>(results, pos, node->items[i].child, lower, upper);
                    }
                }

                bit_pos += BITMAP_WIDTH;
                none_bitmap ++;
            }
            return pos;
        } else {
            int lower_pos = SATISFY_LOWER ? -1 : PREDICT_POS(node, lower);
            int upper_pos = SATISFY_UPPER ? node->num_items : PREDICT_POS(node, upper);
            if constexpr (!SATISFY_LOWER) {
                if (BITMAP_GET(node->none_bitmap, lower_pos) == 0) {
                    if (BITMAP_GET(node->child_bitmap, lower_pos) == 0) {
                        do {
                            if (node->items[lower_pos].key < lower) break;
                            if constexpr (!SATISFY_UPPER) {
                                if (node->items[lower_pos].key > upper) break;
                            }
                            results[pos] = node->items[lower_pos].key;
                            pos ++;
                        } while (false);
                    } else {
                        if (lower_pos < upper_pos) {
                            pos = range_core<false, true>(results, pos, node->items[lower_pos].child, lower, upper);
                        } else {
                            pos = range_core<false, false>(results, pos, node->items[lower_pos].child, lower, upper);
                        }
                    }
                }
            }
            {
                int bit_pos = (lower_pos + 1) / BITMAP_WIDTH * BITMAP_WIDTH;
                const bitmap_t* none_bitmap = node->none_bitmap + bit_pos / BITMAP_WIDTH;
                while (bit_pos < upper_pos) {
                    bitmap_t not_none = ~(*none_bitmap);
                    while (not_none) {
                        int latest_pos = BITMAP_NEXT_1(not_none);
                        not_none ^= 1 << latest_pos;

                        int i = bit_pos + latest_pos;
                        if (i <= lower_pos) continue;
                        if (i >= upper_pos) break;

                        if (BITMAP_GET(node->child_bitmap, i) == 0) {
                            results[pos] = node->items[i].key;
                            // __builtin_prefetch((void*)&(node->items[i].key) + 64);
                            pos ++;
                        } else {
                            pos = range_core<true, true>(results, pos, node->items[i].child, lower, upper);
                        }
                    }

                    bit_pos += BITMAP_WIDTH;
                    none_bitmap ++;
                }
            }
            if constexpr (!SATISFY_UPPER) {
                if (lower_pos < upper_pos) {
                    if (BITMAP_GET(node->none_bitmap, upper_pos) == 0) {
                        if (BITMAP_GET(node->child_bitmap, upper_pos) == 0) {
                            if (node->items[upper_pos].key <= upper) {
                                results[pos] = node->items[upper_pos].key;
                                pos ++;
                            }
                        } else {
                            pos = range_core<true, false>(results, pos, node->items[upper_pos].child, lower, upper);
                        }
                    }
                }
            }
            return pos;
        }
    }
    // SATISFY_LOWER = true means all the keys in the subtree of `node` are no less than to `lower`.
    template<bool SATISFY_LOWER>
    int range_core_len(T* results, int pos, Node* node, const T& lower, int len)
    {
        if constexpr (SATISFY_LOWER) {
            int bit_pos = 0;
            const bitmap_t* none_bitmap = node->none_bitmap;
            while (bit_pos < node->num_items) {
                bitmap_t not_none = ~(*none_bitmap);
                while (not_none) {
                    int latest_pos = BITMAP_NEXT_1(not_none);
                    not_none ^= 1 << latest_pos;

                    int i = bit_pos + latest_pos;
                    if (BITMAP_GET(node->child_bitmap, i) == 0) {
                        results[pos] = node->items[i].key;
                        // __builtin_prefetch((void*)&(node->items[i].key) + 64);
                        pos ++;
                    } else {
                        pos = range_core_len<true>(results, pos, node->items[i].child, lower, len);
                    }
                    if (pos >= len) {
                        return pos;
                    }
                }

                bit_pos += BITMAP_WIDTH;
                none_bitmap ++;
            }
            return pos;
        } else {
            int lower_pos = PREDICT_POS(node, lower);
            if (BITMAP_GET(node->none_bitmap, lower_pos) == 0) {
                if (BITMAP_GET(node->child_bitmap, lower_pos) == 0) {
                    if (node->items[lower_pos].key >= lower) {
                        // results[pos] = {node->items[lower_pos].key, node->items[lower_pos].value};
                        results[pos] = node->items[lower_pos].key;
                        pos ++;
                    }
                } else {
                    pos = range_core_len<false>(results, pos, node->items[lower_pos].child, lower, len);
                }
                if (pos >= len) {
                    return pos;
                }
            }
            if (lower_pos + 1 >= node->num_items) {
                return pos;
            }
            int bit_pos = (lower_pos + 1) / BITMAP_WIDTH * BITMAP_WIDTH;
            const bitmap_t* none_bitmap = node->none_bitmap + bit_pos / BITMAP_WIDTH;
            while (bit_pos < node->num_items) {
                bitmap_t not_none = ~(*none_bitmap);
                while (not_none) {
                    int latest_pos = BITMAP_NEXT_1(not_none);
                    not_none ^= 1 << latest_pos;

                    int i = bit_pos + latest_pos;
                    if (i <= lower_pos) continue;
                    if (BITMAP_GET(node->child_bitmap, i) == 0) {
                        results[pos] = node->items[i].key;
                        // __builtin_prefetch((void*)&(node->items[i].key) + 64);
                        pos ++;
                    } else {
                        pos = range_core_len<true>(results, pos, node->items[i].child, lower, len);
                    }
                    if (pos >= len) {
                        return pos;
                    }
                }
                bit_pos += BITMAP_WIDTH;
                none_bitmap ++;
            }
            return pos;
        }
    }
};

#endif // __LIPP_H__
