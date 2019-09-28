{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Run (run, parseSql) where

import Import

import System.IO

import Text.Show.Pretty (ppShow)

import qualified RIO.Text as T

import Language.SQL.SimpleSQL.Parse
       (parseStatements
       ,ParseError
       ,peFormattedError)

import Language.SQL.SimpleSQL.Syntax (Statement, Statement (..), QueryExpr (..), SetQuantifier,
    ScalarExpr, TableRef, GroupingExpr, SortSpec, Name, TableRef (..), Name(..), ScalarExpr(..))
import Language.SQL.SimpleSQL.Dialect (postgres)


run :: RIO App ()
run = liftIO $ putStrLn $ T.unpack $ parseSql "select * from test"

parseSql :: String -> Text
parseSql src = do
    let parsed :: Either ParseError [Statement]
        parsed = parseStatements postgres "" Nothing src
    either (error . peFormattedError)
           printParsed
           parsed

printParsed :: [Statement] -> Text
printParsed statements = T.intercalate "\n" $ map printStatement statements

printStatement :: Statement -> Text
printStatement statement = case statement of
    SelectStatement queryExpression -> printQueryExpression queryExpression
    _ -> "not supported"

printQueryExpression :: QueryExpr  -> Text
printQueryExpression statement = case statement of
    Select setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst -> printSelectStatement setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst
    _ -> "not supported"

printSelectStatement :: SetQuantifier -> [(ScalarExpr,Maybe Name)] -> [TableRef] -> Maybe ScalarExpr 
    -> [GroupingExpr] -> Maybe ScalarExpr -> [SortSpec] -> Maybe ScalarExpr -> Maybe ScalarExpr -> Text
printSelectStatement setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst  =
    let fromClause = case from of
                    [] -> "empty FROM not supported"
                    [TRSimple [Name _ tableName]] -> T.pack tableName
                    _ -> "not supported" in
    let selectClause = ".select(" <> (T.intercalate ", " (map parseSelectList selectList)) <> ")" in
    fromClause <> selectClause

parseSelectList :: (ScalarExpr,Maybe Name) -> Text
parseSelectList col = case col of
    (_, Just (Name Nothing name)) -> "col(\"" <> T.pack name <> "\""
    (_, Just (Name (Just (name1, name2)) name3)) -> T.pack name1 <> "." <> T.pack name2 <> "." <> T.pack name3
    (Star, Nothing) -> "col(\"*\")"
    (Iden [Name Nothing name], Nothing) -> "col(\"" <> T.pack name <> "\")"
    (expr, Nothing) -> T.pack $ ppShow expr
