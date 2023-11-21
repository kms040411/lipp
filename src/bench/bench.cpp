#include <lipp.h>
#include <iostream>

using namespace std;

int main()
{
    LIPP<string, int, 8> lipp;

    // insert key-values
    lipp.insert("12345678", 1);
    lipp.insert("87654321", 3);

    return 0;
}
