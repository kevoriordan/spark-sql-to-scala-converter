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
    it "basic check" $ doIt "select * from test" `shouldBe` "test.select(col(\"*\"))"
    it "with projected cols" $ doIt "select a, b from test" `shouldBe` "test.select(col(\"a\"), col(\"b\"))"
