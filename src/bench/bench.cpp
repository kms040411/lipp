#include <lipp.h>
#include <iostream>
#include <random>
#include <array>

#define KEY_LEN 16
#define SEED 1234

std::mt19937 gen(SEED);
std::uniform_int_distribution<> dis(32, 127);

std::string random_string(int len) {
    std::string ret;
    for(int i=0; i<len; i++)
        ret.push_back((char)dis(gen));
    return ret;
}

int main()
{
    LIPP<std::string, int, KEY_LEN> lipp;

    // insert key-values
    const int num_key = 100000;
    std::vector<std::pair<std::string, int>> keys;
    for(int i=0; i<num_key; i++) {
        std::cout << i << " / " << num_key << "\r";
        std::string gen_str = random_string(KEY_LEN);
        keys.push_back(std::make_pair(gen_str, 7));
    }
    std::sort(keys.begin(), keys.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    lipp.bulk_load(&keys[0], num_key);

    // int idx=0;
    // for(auto iter=keys.begin(); iter<keys.end(); iter++) {
    //     //std::cout << idx << " / " << num_key << "\n";
    //     std::cout << idx << " - " << iter->first << " - " << iter->second << " / " << lipp.at(iter->first) << "\n";
    //     if(lipp.at(iter->first) != iter->second) std::cout << "ㄴ Wrong!\n";
    //     idx++;
    // }

    #define RANGE_LEN 10
    std::array<std::string, RANGE_LEN> res;
    lipp.range_query_len(res.data(), keys[3000].first, RANGE_LEN);

    std::cout << "Everything worked well\n";

    return 0;
}
