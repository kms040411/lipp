#ifndef __LIPP_BASE_H__
#define __LIPP_BASE_H__

#include <limits>
#include <cmath>
#include <cstdlib>
#include <algorithm>
#include <vector>

#include "mkl.h"
#include "mkl_lapacke.h"

#define USE_BIAS 1

// Linear regression model
template <class T, int LEN>
class LinearModel
{
public:
    double model[LEN + USE_BIAS] = {0.0}; // Model

    LinearModel() = default;
    LinearModel(double *other_model) {
        for(size_t i=0; i<LEN + USE_BIAS; i++) {
            model[i] = other_model[i];
        }
    }
    explicit LinearModel(const LinearModel &other) {
        for(size_t i=0; i<LEN + USE_BIAS; i++) {
            model[i] = other.model[i];
        }
    }

    inline double predict_double(T key) const
    {
        double res = 0;
        for (size_t i=0; i<LEN; i++) {
            res += model[i] * (double)(key[i]);
        }
        if (USE_BIAS) {
            res += model[LEN];
        }
        return res;
    }

    inline int predict(T key) const
    {
        return std::floor(predict_double(key));
    }

    inline void train(std::vector<std::pair<T, double>> &key_idx) {
        size_t m = key_idx.size();
        size_t n = USE_BIAS ? LEN + 1 : LEN;
        double *a = (double *) malloc(sizeof(double) * m * n);
        double *b = (double *) malloc(sizeof(double) * std::max(m, n));
        for (size_t i=0; i<m; i++) {
            for (size_t j=0; j<LEN; j++) {
                a[i * n + j] = (double)(key_idx[i].first[j]);
            }
            if (USE_BIAS) a[i * n + LEN] = 1.0;
            b[i] = (double)(key_idx[i].second);
        }

        LAPACKE_dgels(LAPACK_ROW_MAJOR, 'N', m, n, 1, a, n, b, 1);

        for (size_t j=0; j<LEN; j++) {
            model[j] = b[j];
        }
        if (USE_BIAS) model[LEN] = b[LEN];

        free(a);
        free(b);

        return;
    };

};

#endif // __LIPP_BASE_H__
