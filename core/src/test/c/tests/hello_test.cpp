#include <gtest/gtest.h>
#include <share/vec_agg_vanilla.h>
#include <share/util.h>

TEST(BinarySearchTest, BasicTest) {
    std::vector v = {1, 2, 3, 4, 4, 4, 5, 6, 7, 8};
    int64_t idx = branch_free_search_upper(v.data(), v.size(), 4);
    EXPECT_EQ(6, idx);
}


TEST(BinarySearchTest, MinIntTest) {
    std::vector v = {42, 1, 2, 3, 4, -4, 4, 5, 6, 7, 8};
    auto min = minInt_Vanilla(v.data(), v.size());
    EXPECT_EQ(-4, min);
}

class StateFullTest : public ::testing::Test {
protected:
    void SetUp() override {
        for (int i = 0; i < 10; ++i) {
            data_.push_back(i);
        }
    }

    void TearDown() override {
        data_.clear();
    }

    std::vector<int64_t> data_;
};

TEST_F(StateFullTest, CheckSize) {
    EXPECT_EQ(data_.size(), 10);
}

TEST_F(StateFullTest, SearchSeven) {
    int64_t idx = branch_free_search_upper(data_.data(), data_.size(), 7LL);
    EXPECT_EQ(8, idx);
}
