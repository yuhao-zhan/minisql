#include "buffer/clock_replacer.h"

#include "gtest/gtest.h"

TEST(CLOCKReplacerTest, SampleTest) {
  CLOCKReplacer clock_replacer(7);

  // Scenario: Unpin six elements, i.e., add them to the replacer.
  clock_replacer.Unpin(1);
  clock_replacer.Unpin(2);
  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(5);
  clock_replacer.Unpin(6);
  clock_replacer.Unpin(1); // Unpin 1 again, should have no effect.
  EXPECT_EQ(6, clock_replacer.Size());

  // Scenario: Get three victims from the clock replacer.
  int value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // Scenario: Pin elements in the replacer.
  clock_replacer.Pin(3); // 3 has already been evicted, so pin has no effect.
  clock_replacer.Pin(4);
  EXPECT_EQ(2, clock_replacer.Size());

  // Scenario: Unpin 4 again to make it eligible for eviction.
  clock_replacer.Unpin(4);

  // Scenario: Continue looking for victims. Expect these victims.
  clock_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(4, value);
}
