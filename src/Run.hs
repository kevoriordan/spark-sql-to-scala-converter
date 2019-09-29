{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE LambdaCase #-}

module Run (run, parseSql) where

import Import

import System.IO

import Text.RawString.QQ

import Text.Show.Pretty (ppShow)

import qualified RIO.Text as T

import Language.SQL.SimpleSQL.Parse
       (parseStatements
       ,ParseError
       ,peFormattedError)

import Language.SQL.SimpleSQL.Syntax (Statement, Statement (..), QueryExpr (..), SetQuantifier,
    ScalarExpr, TableRef, GroupingExpr, SortSpec, Name, TableRef (..), Name(..), ScalarExpr(..),
    TypeName, TypeName (..))
import Language.SQL.SimpleSQL.Dialect (postgres)


run :: RIO MyApp ()
run = liftIO $ putStrLn $ T.unpack $ parseSql [r|
SELECT blah FROM blah2
|]

parseSql :: String -> Text
parseSql src = do
    let parsed :: Either ParseError [Statement]
        parsed = parseStatements postgres "" Nothing src
    either (error . peFormattedError)
           parseStmts
           parsed

parseStmts :: [Statement] -> Text
parseStmts statements = T.intercalate "\n" $ map parseStatement statements

parseStatement :: Statement -> Text
parseStatement statement = case statement of
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
                    [TRSimple name] -> createForBlock $ parseNames name
                    blah -> T.pack (ppShow blah)  in
    let whereClause = case sWhere of 
                        Nothing -> ""
                        Just expr -> "\n  .filter(" <> parseScalarExpression expr <> ")" in
    let selectClause = "\n  .select(" <> (T.intercalate ", " (map parseSelectList selectList)) <> ")" in
    fromClause <> whereClause <> selectClause

createForBlock :: Text -> Text
createForBlock tableName = 
    "val " <> cleanedTableName <> " = spark.table(\"" <> tableName <> "\")\n" <> cleanedTableName
    where 
        cleanedTableName = T.map (\case '.' -> '_'; other -> other) tableName

parseSelectList :: (ScalarExpr,Maybe Name) -> Text
parseSelectList col = case col of
    (expr, Just name) -> parseScalarExpression expr <> ".as(\"" <> parseNames [name] <> "\")"
    (expr, Nothing) -> parseScalarExpression expr


parseScalarExpressions :: [ScalarExpr] -> Text
parseScalarExpressions exprs = T.intercalate ", " (map parseScalarExpression exprs)

parseScalarExpression :: ScalarExpr -> Text
parseScalarExpression expr = case expr of
    Star -> "col(\"*\")"
    Iden name -> "col(\"" <> parseNames name <> "\")"
    App functionNames subExprs ->  parseFunctionNames functionNames <> "(" <> parseScalarExpressions subExprs <> ")"
    Cast subExpr typeName -> parseScalarExpression subExpr <> ".cast(" <> sparkType typeName <> ")"
    Case test whens elsePart -> parseCaseExpression test whens elsePart 
    BinOp subExpr1 funcNames subExpr2 -> parseScalarExpression subExpr1 <> " " <> parseFunctionNames funcNames <> " " <> parseScalarExpression subExpr2
    PostfixOp functionNames subExpr -> parseScalarExpression subExpr <> parseFunctionNames functionNames
    SpecialOp functionNames subExprs -> handleSpecialOp functionNames subExprs
    NumLit lit -> T.pack lit
    StringLit _ _ lit -> "\"" <> T.pack lit <> "\""
    unparsed -> T.pack $ ppShow unparsed 


-- Currently used to handle between
handleSpecialOp :: [Name] -> [ScalarExpr] -> Text
handleSpecialOp functionNames exprs = case functionNames of
    [Name Nothing "between"] -> handleBetween exprs
    _ -> "ERROR"


handleBetween :: [ScalarExpr] -> Text
handleBetween exprs = case exprs of 
    [expr1, expr2, expr3] ->  parseScalarExpression expr1
                                    <> ".between(" <> parseScalarExpression expr2 <> ", " 
                                    <> parseScalarExpression expr3 <> ")"
    _ -> "unhandled"
   

parseFunctionNames :: [Name] -> Text
parseFunctionNames names = case names of
    [name] -> parseFunctionName name
    [schemaName, name] -> parseName schemaName <> "." <> parseFunctionName name
    other -> T.pack $ ppShow other

parseFunctionName :: Name -> Text
parseFunctionName name = case name of
    Name Nothing subName -> sparkFunction $ T.pack subName
    other -> T.pack $ ppShow other

parseNames :: [Name] -> Text
parseNames names = case names of
    [name] -> parseName name
    [schemaName, name] -> parseName schemaName <> "." <> parseName name
    other -> T.pack $ ppShow other

parseName :: Name -> Text
parseName name = case name of
    Name Nothing subName -> T.pack subName
    other -> T.pack $ ppShow other

parseCaseExpression :: Maybe ScalarExpr -> [([ScalarExpr],ScalarExpr)] -> Maybe ScalarExpr -> Text
parseCaseExpression _ whens elsePart =
    T.intercalate "." (map parseWhen whens) <> parseElse
    where
        parseWhen (condExprs, thenExpr) = "when(" <> parseScalarExpressions condExprs <> ", " <> parseScalarExpression thenExpr <> ")"
        parseElse = case elsePart of
            Nothing -> ""
            Just expr -> ".otherwise(" <> parseScalarExpression expr <> ")"


sparkFunction :: Text -> Text
sparkFunction function = case T.toLower function of
    "nvl" -> "coalesce"
    "strpos" -> "instr"
    "is null" -> ".isNull()"
    "=" -> "==="
    other -> T.toLower other

sparkType :: TypeName -> Text
sparkType typeName = case typeName of
    TypeName name -> T.pack $ ppShow name
    PrecTypeName [Name Nothing "varchar"] _ -> "DataTypes.StringType"
    blah -> T.pack $ ppShow blah