{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module BasicSpec (spec) where

import Import
import Run
import Test.Hspec
import Test.Hspec.QuickCheck

spec :: Spec
spec = do
  describe "sql checks" $ do
    it "basic check" $ parseSql "select * from test" `shouldBe` "val test = spark.table(\"test\")\ntest\n  .select(col(\"*\"))"
    it "with projected cols" $ parseSql "select a, b from test" `shouldBe` "val test = spark.table(\"test\")\ntest\n  .select(col(\"a\"), col(\"b\"))"
