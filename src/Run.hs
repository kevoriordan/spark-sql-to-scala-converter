{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveGeneric #-}

module Run
    ( run
    , parseSql
    )
where

import           Text.RawString.QQ

import           Text.Show.Pretty               ( ppShow )

import qualified Data.Text                     as T
import           Data.Text                      ( Text )

import           Language.SQL.SimpleSQL.Parse   ( parseStatements
                                                , ParseError
                                                , peFormattedError
                                                )

import           Language.SQL.SimpleSQL.Syntax  ( Statement
                                                , Statement(..)
                                                , QueryExpr(..)
                                                , SetQuantifier
                                                , ScalarExpr
                                                , TableRef
                                                , GroupingExpr
                                                , SortSpec
                                                , Name
                                                , TableRef(..)
                                                , Name(..)
                                                , ScalarExpr(..)
                                                , TypeName
                                                , TypeName(..)
                                                , InsertSource
                                                , InsertSource(..)
                                                , SetQuantifier(..)
                                                , Alias
                                                , Alias(..)
                                                , InPredValue
                                                , InPredValue(..)
                                                , GroupingExpr(..)
                                                , JoinType
                                                , JoinType(..)
                                                , JoinCondition
                                                , JoinCondition(..)
                                                , Frame
                                                , SortSpec
                                                , SortSpec(..)
                                                , Direction(..)
                                                , NullsOrder(..)
                                                )
import           Language.SQL.SimpleSQL.Dialect ( postgres )


run :: IO ()
run = putStrLn $ T.unpack $ parseSql [r|
select A.* from A cross join B order by b desc nulls last
|]


parseSql :: String -> Text
parseSql src = do
    let parsed :: Either ParseError [Statement]
        parsed = parseStatements postgres "" Nothing src
    either (error . peFormattedError) parseStmts parsed

parseStmts :: [Statement] -> Text
parseStmts statements = T.intercalate "\n" $ map parseStatement statements

parseStatement :: Statement -> Text
parseStatement statement = case statement of
    SelectStatement queryExpression -> printQueryExpression queryExpression
    Insert names _ insertSource     -> parseInsertStatement names insertSource
    DropTable   _ _                 -> ""
    CreateTable _ _                 -> ""
    blah                            -> T.pack (ppShow blah)

parseWithBlocks :: [Name] -> [(Alias, QueryExpr)] -> QueryExpr -> Text
parseWithBlocks insertIntoNames views queryExpression =
    T.intercalate "\n" (map parseWithBlock views)
        <> "\n"
        <> printInsertQueryExpression insertIntoNames queryExpression

parseWithBlock :: (Alias, QueryExpr) -> Text
parseWithBlock (alias, queryExpr) =
    "val " <> parseTableAlias alias <> " = " <> printQueryExpression queryExpr

parseTableAlias :: Alias -> Text
parseTableAlias alias = case alias of
    Alias name Nothing -> parseName name
    other              -> T.pack $ ppShow other


parseInsertStatement :: [Name] -> InsertSource -> Text
parseInsertStatement insertIntoNames insertSource = case insertSource of
    InsertQuery queryExpr ->
        printInsertQueryExpression insertIntoNames queryExpr
    blah -> T.pack (ppShow blah)


scalaFormatTableName :: Text -> Text
scalaFormatTableName = T.map
    (\case
        '.'   -> '_'
        other -> other
    )


printInsertQueryExpression :: [Name] -> QueryExpr -> Text
printInsertQueryExpression insertIntoNames expression = case expression of
    Select setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst
        -> "val "
            <> parseNamesInsideIden insertIntoNames
            <> " = "
            <> printSelectStatement setQuantifier
                                    selectList
                                    from
                                    sWhere
                                    groupBy
                                    having
                                    orderBy
                                    offset
                                    fetchFirst
    With _ views queryExpression ->
        parseWithBlocks insertIntoNames views queryExpression
    other -> T.pack $ ppShow other

printQueryExpression :: QueryExpr -> Text
printQueryExpression expression = case expression of
    Select setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst
        -> printSelectStatement setQuantifier
                                selectList
                                from
                                sWhere
                                groupBy
                                having
                                orderBy
                                offset
                                fetchFirst
    other -> T.pack $ ppShow other

printSelectStatement
    :: SetQuantifier
    -> [(ScalarExpr, Maybe Name)]
    -> [TableRef]
    -> Maybe ScalarExpr
    -> [GroupingExpr]
    -> Maybe ScalarExpr
    -> [SortSpec]
    -> Maybe ScalarExpr
    -> Maybe ScalarExpr
    -> Text
printSelectStatement setQuantifier selectList from sWhere groupBy having orderBy _ _
    = let fromClause = parseFromClause from
      in
          let
              whereClause = case sWhere of
                  Nothing -> ""
                  Just expr ->
                      "\n  .filter(" <> parseScalarExpressionToAny expr <> ")"
          in
              let groupByClause = parseGroupingExpressions groupBy
              in
                  let selectClause = parseSelectClause groupByClause selectList
                  in
                      let
                          havingClause = case having of
                              Nothing -> ""
                              Just expr ->
                                  "\n  .filter("
                                      <> parseScalarExpressionToAny expr
                                      <> ")"
                      in
                          let sqClause = case setQuantifier of
                                  SQDefault -> ""
                                  All       -> ""
                                  Distinct  -> "\n  .distinct()"
                          in
                              let
                                  orderByClause = case orderBy of
                                      [] -> ""
                                      _ ->
                                          "\n  .orderBy("
                                              <> parseSortSpecs orderBy
                                              <> ")"
                              in  fromClause
                                  <> whereClause
                                  <> groupByClause
                                  <> selectClause
                                  <> orderByClause
                                  <> havingClause
                                  <> sqClause

parseFromClause :: [TableRef] -> Text
parseFromClause tableRefs = case tableRefs of
    []            -> "empty FROM not supported"
    tableRef : xs -> parseTableRef tableRef
        <> T.concat (map (\x -> "\n  .join(" <> parseTableRef x <> ")") xs)

parseTableRef :: TableRef -> Text
parseTableRef tableRef = case tableRef of
    TRJoin tableA isNatural joinType tableB maybeJoinCondition ->
        parseTableJoin tableA isNatural joinType tableB maybeJoinCondition
    TRSimple name -> scalaFormatTableName $ parseNamesInsideIden name
    TRAlias subRef alias ->
        parseTableRef subRef <> ".as(\"" <> parseAlias alias <> "\")"
    blah -> T.pack $ ppShow blah

parseTableJoin
    :: TableRef -> Bool -> JoinType -> TableRef -> Maybe JoinCondition -> Text
parseTableJoin tableA _ joinType tableB maybeJoinCondition =
    parseTableRef tableA
        <> "\n  ."
        <> joinKeyword
        <> "("
        <> parseTableRef tableB
        <> parseJoinCondition maybeJoinCondition
        <> parseJoinType joinType
        <> ")"
  where
    joinKeyword = case joinType of
        JCross -> "crossJoin"
        _      -> "join"

parseJoinType :: JoinType -> Text
parseJoinType joinType = case joinType of
    JInner -> ""
    JLeft  -> ", \"left\""
    JRight -> ", \"right\""
    JFull  -> ", \"outer\""
    JCross -> ""  -- the cross join condition is handled with .crossJoin instead of .join in Spark

parseJoinCondition :: Maybe JoinCondition -> Text
parseJoinCondition Nothing              = ""
parseJoinCondition (Just joinCondition) = case joinCondition of
    JoinOn    expr  -> ", " <> parseScalarExpressionToAny expr
    JoinUsing names -> ", Seq(" <> parseNamesInUsingClause names <> ")"

parseAlias :: Alias -> Text
parseAlias (Alias name _) = parseName name

parseSelectClause :: Text -> [(ScalarExpr, Maybe Name)] -> Text
parseSelectClause groupByClause selectList = case groupByClause of
    "" ->
        "\n  .select("
            <> T.intercalate ", " (map parseSelectList selectList)
            <> ")"
    _ ->
        "\n  .agg("
            <> T.intercalate "," (map parseSelectList (filter isAgg selectList))
            <> ")"

isAgg :: (ScalarExpr, Maybe Name) -> Bool
isAgg (expr, _) = case expr of
    App _ _ -> True
    _       -> False


parseGroupingExpressions :: [GroupingExpr] -> Text
parseGroupingExpressions groupingExpressions = case groupingExpressions of
    [] -> ""
    other ->
        "\n  .groupBy("
            <> T.intercalate "," (map parseGroupingExpression other)
            <> ")"

parseGroupingExpression :: GroupingExpr -> Text
parseGroupingExpression groupingExpr = case groupingExpr of
    SimpleGroup scalarExpr -> parseScalarExpressionToCol scalarExpr
    other                  -> T.pack $ ppShow other

parseSelectList :: (ScalarExpr, Maybe Name) -> Text
parseSelectList (expr, alias) =
    parseScalarExpressionToCol expr <> case alias of
        Just name -> ".as(\"" <> parseNamesInsideIden [name] <> "\")"
        Nothing   -> ""


parseScalarExpressionsGeneral :: [ScalarExpr] -> Text
parseScalarExpressionsGeneral exprs =
    T.intercalate ", " (map parseScalarExpressionToAny exprs)


parseScalarExpressionsSecArgCol :: [ScalarExpr] -> Text
parseScalarExpressionsSecArgCol exprs =
    T.intercalate ", " (map parseScalarExpressionToCol exprs)


parseScalarExpressionToCol :: ScalarExpr -> Text
parseScalarExpressionToCol expr = case expr of
    NumLit lit        -> "lit(" <> T.pack lit <> ")"
    StringLit _ _ lit -> "lit(\"" <> T.pack lit <> "\")"
    Iden name         -> parseIdenToCol name
    other             -> _parseScalarExpression other


parseScalarExpressionToAny :: ScalarExpr -> Text
parseScalarExpressionToAny expr = case expr of
    NumLit lit        -> T.pack lit
    StringLit _ _ lit -> "\"" <> T.pack lit <> "\""
    Iden name         -> parseIdenToAny name
    other             -> _parseScalarExpression other


parseScalarExpressionNoCol :: ScalarExpr -> Text
parseScalarExpressionNoCol expr = case expr of
    NumLit lit        -> T.pack lit
    StringLit _ _ lit -> "\"" <> T.pack lit <> "\""
    Iden name         -> parseIdenNoCol name
    other             -> _parseScalarExpression other

_parseScalarExpression :: ScalarExpr -> Text
_parseScalarExpression expr = case expr of
    Star                       -> "col(\"*\")"
    App functionNames subExprs -> parseAppExpression functionNames subExprs
    Cast subExpr typeName ->
        parseScalarExpressionToCol subExpr
            <> ".cast("
            <> sparkType typeName
            <> ")"
    Case  test     whens     elsePart -> parseCaseExpression test whens elsePart
    BinOp subExpr1 funcNames subExpr2 -> parseBinOp subExpr1 funcNames subExpr2
    PostfixOp functionNames subExpr ->
        parseScalarExpressionToCol subExpr <> parseFunctionNames functionNames
    SpecialOp functionNames subExprs -> handleSpecialOp functionNames subExprs
    PrefixOp functionNames subExpr ->
        parseFunctionNames functionNames <> parseScalarExpressionToCol subExpr
    Parens subExpr       -> "(" <> parseScalarExpressionToCol subExpr <> ")"
    HostParameter name _ -> T.pack name
    In inOrNotIn subExpr predValue ->
        parseInExpression inOrNotIn subExpr predValue
    WindowApp names args partition orderBy frame ->
        parseWindowFunction names args partition orderBy frame
    unparsed -> T.pack $ ppShow unparsed

parseSortSpecs :: [SortSpec] -> Text
parseSortSpecs sortSpecs = T.intercalate "," $ map parseSortSpec sortSpecs

parseSortSpec :: SortSpec -> Text
parseSortSpec (SortSpec expr direction nullsOrder) =
    case (direction, nullsOrder) of
        (DirDefault, NullsOrderDefault) -> parseScalarExpressionToCol expr
        (Asc, NullsOrderDefault) ->
            "asc(" <> parseScalarExpressionNoCol expr <> ")"
        (Desc, NullsOrderDefault) ->
            "desc(" <> parseScalarExpressionNoCol expr <> ")"
        (DirDefault, NullsFirst) ->
            parseScalarExpressionToCol expr <> ".asc_nulls_first()"
        (Asc, NullsFirst) ->
            parseScalarExpressionToCol expr <> ".asc_nulls_first()"
        (Desc, NullsFirst) ->
            parseScalarExpressionToCol expr <> ".desc_nulls_first()"
        (DirDefault, NullsLast) ->
            parseScalarExpressionToCol expr <> ".asc_nulls_last()"
        (Asc, NullsLast) ->
            parseScalarExpressionToCol expr <> ".asc_nulls_last()"
        (Desc, NullsLast) ->
            parseScalarExpressionToCol expr <> ".desc_nulls_last()"

parseWindowFunction
    :: [Name]
    -> [ScalarExpr]
    -> [ScalarExpr]
    -> [SortSpec]
    -> Maybe Frame
    -> Text
parseWindowFunction names _ partition orderBy _ =
    parseNamesInsideIden names
        <> "().over(Window.partitionBy("
        <> parseScalarExpressionsGeneral partition
        <> ").orderBy("
        <> parseSortSpecs orderBy
        <> ")"

parseInExpression :: Bool -> ScalarExpr -> InPredValue -> Text
parseInExpression inOrNotIn expr predValue =
    prefix
        <> parseScalarExpressionToCol expr
        <> ".isin("
        <> parseInPredicate predValue
        <> ")"
    where prefix = if inOrNotIn then "" else "!"

parseInPredicate :: InPredValue -> Text
parseInPredicate predValue = case predValue of
    InList      expr -> parseScalarExpressionsGeneral expr
    InQueryExpr expr -> printQueryExpression expr

parseBinOp :: ScalarExpr -> [Name] -> ScalarExpr -> Text
parseBinOp subExpr1 funcNames subExpr2 =
    let functionName = parseFunctionNames funcNames
    in  case functionName of
            "not like" -> "!" <> parsedExpr1 <> ".like(" <> parsedExpr2 <> ")"
            "like"     -> parsedExpr1 <> ".like(" <> parsedExpr2 <> ")"
            -- This is where the AST gets fucked up, thinks a schema dot separator is a function
            "."        -> handleSpecialCaseForDotSeperator subExpr1 subExpr2
            _ -> parsedExpr1 <> " " <> functionName <> " " <> parsedExpr2
  where
    parsedExpr1 = parseScalarExpressionToCol subExpr1
    parsedExpr2 = parseScalarExpressionToAny subExpr2


handleSpecialCaseForDotSeperator :: ScalarExpr -> ScalarExpr -> Text
handleSpecialCaseForDotSeperator expr1 expr2 =
    "col(\"" <> parsedTableAndColumn <> "\")"
  where
    parsedTableAndColumn = case (expr1, expr2) of
        (Iden name1, Iden name2) ->
            parseNamesInsideIden name1 <> "." <> parseNamesInsideIden name2
        (Iden name1, Star) -> parseNamesInsideIden name1 <> ".*"
        _                  -> "error"

parseIdenToCol :: [Name] -> Text
parseIdenToCol names =
    let parsedName = parseNamesInsideIden names
    in  case parsedName of
            "TRUE"  -> "lit(true)"
            "FALSE" -> "lit(false)"
            "NULL" -> "lit(null)"
            other   -> "col(\"" <> other <> "\")"

parseIdenToAny :: [Name] -> Text
parseIdenToAny names =
    let parsedName = parseNamesInsideIden names
    in  case parsedName of
            "TRUE"  -> "true"
            "FALSE" -> "false"
            "NULL" -> "null"
            other   -> "col(\"" <> other <> "\")"

parseIdenNoCol :: [Name] -> Text
parseIdenNoCol names = 
    let parsedName = parseNamesInUsingClause names
    in  case parsedName of
        "TRUE"  -> "true"
        "FALSE" -> "false"
        "NULL" -> "null"
        other   -> other


parseAppExpression :: [Name] -> [ScalarExpr] -> Text
parseAppExpression functionNames subExpr =
    let functionName = parseFunctionNames functionNames
    in
        case functionName of
            "coalesce" ->
                "coalesce(" <> parseScalarExpressionsSecArgCol subExpr <> ")"
            "greatest" ->
                "greatest(" <> parseScalarExpressionsSecArgCol subExpr <> ")"
            "least" ->
                "least(" <> parseScalarExpressionsSecArgCol subExpr <> ")"
            "date_part" -> parseDatePart subExpr
            _ ->
                functionName
                    <> "("
                    <> parseScalarExpressionsGeneral subExpr
                    <> ")"

parseDatePart :: [ScalarExpr] -> Text
parseDatePart [expr1, expr2] = 
    case firstExpr of 
        "\"month\"" -> "month(" <> secondExpr <> ")"
        "\"day\"" -> "day(" <> secondExpr <> ")"
        "\"year\"" -> "year(" <> secondExpr <> ")"
        _ -> "parseError " <> firstExpr
    where 
        firstExpr = parseScalarExpressionNoCol expr1
        secondExpr = parseScalarExpressionToCol expr2
parseDatePart _ = "parseError"

-- Currently used to handle between
handleSpecialOp :: [Name] -> [ScalarExpr] -> Text
handleSpecialOp functionNames exprs = case functionNames of
    [Name Nothing "between"] -> handleBetween exprs
    unparsed                 -> T.pack $ ppShow unparsed


handleBetween :: [ScalarExpr] -> Text
handleBetween exprs = case exprs of
    [expr1, expr2, expr3] ->
        parseScalarExpressionToCol expr1
            <> ".between("
            <> parseScalarExpressionToAny expr2
            <> ", "
            <> parseScalarExpressionToAny expr3
            <> ")"
    _ -> "unhandled"


parseFunctionNames :: [Name] -> Text
parseFunctionNames names = case names of
    [name]             -> parseFunctionName name
    [schemaName, name] -> parseName schemaName <> "." <> parseFunctionName name
    other              -> T.pack $ ppShow other

parseFunctionName :: Name -> Text
parseFunctionName name = case name of
    Name Nothing subName -> sparkFunction $ T.pack subName
    other                -> T.pack $ ppShow other

parseNamesInUsingClause :: [Name] -> Text
parseNamesInUsingClause names =
    T.intercalate "," $ map (\x -> "\"" <> parseName x <> "\"") names

parseNamesInsideIden :: [Name] -> Text
parseNamesInsideIden names = case names of
    [name]             -> parseName name
    [schemaName, name] -> parseName schemaName <> "." <> parseName name
    other              -> T.pack $ ppShow other

parseName :: Name -> Text
parseName name = case name of
    Name Nothing subName -> T.pack subName
    other                -> T.pack (ppShow other)

parseCaseExpression
    :: Maybe ScalarExpr
    -> [([ScalarExpr], ScalarExpr)]
    -> Maybe ScalarExpr
    -> Text
parseCaseExpression _ whens elsePart =
    T.intercalate "." (map parseWhen whens) <> parseElse
  where
    parseWhen (condExprs, thenExpr) =
        "when("
            <> parseScalarExpressionsGeneral condExprs
            <> ", "
            <> parseScalarExpressionToAny thenExpr
            <> ")"
    parseElse = case elsePart of
        Nothing   -> ""
        Just expr -> ".otherwise(" <> parseScalarExpressionToAny expr <> ")"


sparkFunction :: Text -> Text
sparkFunction function = case T.toLower function of
    "nvl"          -> "coalesce"
    "strpos"       -> "instr"
    "is null"      -> ".isNull()"
    "is not null"  -> ".isNotNull()"
    "="            -> "==="
    "<>"           -> "=!="
    "is true"      -> " === true"
    "is false"     -> " === false"
    "is not true"  -> " =!= true"
    "is not false" -> " =!= false"
    "not"          -> "!"
    other          -> T.toLower other

sparkType :: TypeName -> Text
sparkType typeName = case typeName of
    TypeName [Name Nothing "int8"] -> "DataTypes.LongType"
    TypeName [name               ] -> T.pack $ ppShow name
    PrecTypeName [Name Nothing "varchar"] _ -> "DataTypes.StringType"
    blah                           -> T.pack $ ppShow blah
