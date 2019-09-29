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
    TypeName, TypeName (..), InsertSource, InsertSource(..), SetQuantifier(..))
import Language.SQL.SimpleSQL.Dialect (postgres)


run :: RIO MyApp ()
run = liftIO $ putStrLn $ T.unpack $ parseSql [r|
  select a, b from blah
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
    Insert names _ insertSource -> parseInsertStatement names insertSource
    DropTable _ _ -> ""
    CreateTable _ _ -> ""
    blah -> T.pack (ppShow blah)


parseInsertStatement :: [Name] -> InsertSource -> Text
parseInsertStatement names insertSource =
    let fromTable = parseFromTable insertSource in
    "val " <> scalaFormatTableName fromTable <> " = spark.table(\"" <> fromTable <> "\")\n" <> 
       "val " <> scalaFormatTableName (parseNames names) <> " = " <> parseInsertSource insertSource


parseFromTable :: InsertSource -> Text
parseFromTable insertSource = case insertSource of
    InsertQuery queryExpr -> parseFromQueryExpr queryExpr
    _ -> ""
    where 
        parseFromQueryExpr expr = case expr of
            Select setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst ->
                case from of
                    [TRSimple name] -> parseNames name

scalaFormatTableName :: Text -> Text
scalaFormatTableName = T.map (\case '.' -> '_'; other -> other)

parseInsertSource :: InsertSource -> Text
parseInsertSource insertSource = case insertSource of
    InsertQuery queryExpr -> printQueryExpression queryExpr
    blah -> T.pack (ppShow blah)


printQueryExpression :: QueryExpr  -> Text
printQueryExpression statement = case statement of
    Select setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst -> printSelectStatement setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst
    _ -> "not supported"

printSelectStatement :: SetQuantifier -> [(ScalarExpr,Maybe Name)] -> [TableRef] -> Maybe ScalarExpr 
    -> [GroupingExpr] -> Maybe ScalarExpr -> [SortSpec] -> Maybe ScalarExpr -> Maybe ScalarExpr -> Text
printSelectStatement setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst  =
    let fromClause = case from of
                    [] -> "empty FROM not supported"
                    [TRSimple name] -> scalaFormatTableName $ parseNames name
                    blah -> T.pack (ppShow blah)  in
    let whereClause = case sWhere of 
                        Nothing -> ""
                        Just expr -> "\n  .filter(" <> parseScalarExpressionToAny expr <> ")" in
    let selectClause = "\n  .select(" <> (T.intercalate ", " (map parseSelectList selectList)) <> ")" in
        let sqClause = case setQuantifier of
                            SQDefault -> ""
                            All -> ""
                            Distinct -> "\n  .distinct()" in
    fromClause <> whereClause <> selectClause <> sqClause

parseSelectList :: (ScalarExpr,Maybe Name) -> Text
parseSelectList col = case col of
    (expr, Just name) -> parseScalarExpressionToCol expr <> ".as(\"" <> parseNames [name] <> "\")"
    (expr, Nothing) -> parseScalarExpressionToCol expr


parseScalarExpressionsGeneral :: [ScalarExpr] -> Text
parseScalarExpressionsGeneral exprs = T.intercalate ", " (map parseScalarExpressionToAny exprs)


parseScalarExpressionsSecArgCol :: [ScalarExpr] -> Text
parseScalarExpressionsSecArgCol exprs = T.intercalate ", " (map parseScalarExpressionToCol exprs)


parseScalarExpressionToCol :: ScalarExpr -> Text
parseScalarExpressionToCol expr = case expr of
    NumLit lit -> "lit(" <> T.pack lit <> ")"
    StringLit _ _ lit -> "lit(\"" <> T.pack lit <> "\")"
    Iden name -> parseIdenToCol name
    other -> parseScalarExpression other


parseScalarExpressionToAny :: ScalarExpr -> Text
parseScalarExpressionToAny expr = case expr of
    NumLit lit -> T.pack lit
    StringLit _ _ lit -> "\"" <> T.pack lit <> "\""
    Iden name -> parseIdenToAny name
    other -> parseScalarExpression other


parseScalarExpression :: ScalarExpr -> Text
parseScalarExpression expr = case expr of
    Star -> "col(\"*\")"
    App functionNames subExprs ->  parseAppExpression functionNames subExprs
    Cast subExpr typeName -> parseScalarExpressionToCol subExpr <> ".cast(" <> sparkType typeName <> ")"
    Case test whens elsePart -> parseCaseExpression test whens elsePart 
    BinOp subExpr1 funcNames subExpr2 -> parseBinOp subExpr1 funcNames subExpr2
    PostfixOp functionNames subExpr -> parseScalarExpressionToCol subExpr <> parseFunctionNames functionNames
    SpecialOp functionNames subExprs -> handleSpecialOp functionNames subExprs
    Parens subExpr -> "(" <> parseScalarExpressionToCol subExpr <> ")"
    HostParameter name _ -> T.pack name
    unparsed -> T.pack $ ppShow unparsed 


parseBinOp :: ScalarExpr -> [Name] -> ScalarExpr -> Text
parseBinOp subExpr1 funcNames subExpr2 = 
    let functionName = parseFunctionNames funcNames in
        case functionName of
            "not like" -> "!" <> parsedExpr1 <> ".like(" <> parsedExpr2 <> ")"
            "like" -> parsedExpr1 <> ".like(" <> parsedExpr2 <> ")"
            _ -> parsedExpr1 <> " " <> functionName <> " " <> parsedExpr2
    where
        parsedExpr1 = parseScalarExpressionToCol subExpr1
        parsedExpr2 = parseScalarExpressionToAny subExpr2
    

parseIdenToCol :: [Name] -> Text
parseIdenToCol names =
    let parsedName = parseNames names in
        case parsedName of
            "TRUE" -> "lit(true)"
            "FALSE" -> "lit(false)"
            other -> "col(\"" <> other <> "\")"

parseIdenToAny :: [Name] -> Text
parseIdenToAny names =
    let parsedName = parseNames names in
        case parsedName of
            "TRUE" -> "true"
            "FALSE" -> "false"
            other -> "col(\"" <> other <> "\")"

parseAppExpression :: [Name] -> [ScalarExpr] -> Text
parseAppExpression functionNames subExpr = 
    let functionName = parseFunctionNames functionNames in
    case functionName of
        "coalesce" -> "coalesce(" <> parseScalarExpressionsSecArgCol subExpr <> ")"
        _ -> functionName <> "(" <> parseScalarExpressionsGeneral subExpr <> ")"

-- Currently used to handle between
handleSpecialOp :: [Name] -> [ScalarExpr] -> Text
handleSpecialOp functionNames exprs = case functionNames of
    [Name Nothing "between"] -> handleBetween exprs
    unparsed -> T.pack $ ppShow unparsed


handleBetween :: [ScalarExpr] -> Text
handleBetween exprs = case exprs of 
    [expr1, expr2, expr3] ->  parseScalarExpressionToCol expr1
                                    <> ".between(" <> parseScalarExpressionToAny expr2 <> ", " 
                                    <> parseScalarExpressionToAny expr3 <> ")"
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
        parseWhen (condExprs, thenExpr) = "when(" <> parseScalarExpressionsGeneral condExprs <> ", " <> parseScalarExpressionToAny thenExpr <> ")"
        parseElse = case elsePart of
            Nothing -> ""
            Just expr -> ".otherwise(" <> parseScalarExpressionToAny expr <> ")"


sparkFunction :: Text -> Text
sparkFunction function = case T.toLower function of
    "nvl" -> "coalesce"
    "strpos" -> "instr"
    "is null" -> ".isNull()"
    "=" -> "==="
    "is true" -> " === true"
    "is false" -> " === false"
    "is not true" -> " =!= true"
    "is not false" -> " =!= false"
    other -> T.toLower other

sparkType :: TypeName -> Text
sparkType typeName = case typeName of
    TypeName name -> T.pack $ ppShow name
    PrecTypeName [Name Nothing "varchar"] _ -> "DataTypes.StringType"
    blah -> T.pack $ ppShow blah