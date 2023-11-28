#include <lipp.h>
#include <iostream>
#include <random>

#define KEY_LEN 8
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
    lipp.insert("12345678", 1);
    lipp.insert("87654321", 3);
    std::cout << lipp.at("12345678") << ", " << lipp.at("87654321") << std::endl;
    RT_ASSERT(lipp.at("12345678") == 1);
    RT_ASSERT(lipp.at("87654321") == 3);

    const int num_key = 1000000;
    std::vector<std::pair<std::string, int>> keys;
    for(int i=0; i<num_key; i++) {
        std::string gen_str = random_string(KEY_LEN);
        keys.push_back(std::make_pair(gen_str, 1));
    }
    std::sort(keys.begin(), keys.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    lipp.bulk_load(&keys[0], num_key);

    std::cout << "Everything worked well\n";
    
    return 0;
}
