{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module Run
    ( run
    , parseSql
    )
where

import           Data.Either()
import           System.Environment             (getArgs)

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

data UnhandledError = UnhandledError
                        {
                            uhErrorString :: Text,
                            uhOriginalPP :: Text
                        } deriving (Show)

run :: IO ()
run = do
  args <- getArgs
  case args of
    [] -> putStrLn "Error: Must specify filename of SQL to convert"
    [filename] -> parseFile filename
    _ -> putStrLn "Too many arguments. Only filename is supported"

parseFile :: String -> IO ()
parseFile filename = do
  sqlString <- readFile filename
  let parsedSql = parseSql sqlString in
    case parsedSql of
      Right parsed -> putStrLn $ T.unpack parsed
      Left err -> print err


parseSql :: String -> Either UnhandledError Text
parseSql src = do
    let parsed :: Either ParseError [Statement]
        parsed = parseStatements postgres "" Nothing src
    either (error . peFormattedError) parseStmts parsed

parseStmts :: [Statement] -> Either UnhandledError Text
parseStmts statements = do
    parsedStatements <- traverse parseStatement statements
    pure $ T.intercalate "\n" parsedStatements

parseStatement :: Statement -> Either UnhandledError Text
parseStatement statement = case statement of
    SelectStatement queryExpression -> printQueryExpression queryExpression
    Insert names _ insertSource     -> parseInsertStatement names insertSource
    DropTable   _ _                 -> Right ""
    CreateTable _ _                 -> Right ""
    blah -> parseError "Can't parse top level statement" blah

parseWithBlocks
    :: [Name] -> [(Alias, QueryExpr)] -> QueryExpr -> Either UnhandledError Text
parseWithBlocks insertIntoNames views queryExpression = do
    parsedViews       <- traverse parseWithBlock views
    parsedInsertQuery <- printInsertQueryExpression insertIntoNames
                                                    queryExpression
    pure $ T.intercalate "\n" parsedViews <> "\n" <> parsedInsertQuery

parseWithBlock :: (Alias, QueryExpr) -> Either UnhandledError Text
parseWithBlock (alias, queryExpr) = do
    parsedAlias     <- parseTableAlias alias
    parsedQueryExpr <- printQueryExpression queryExpr
    pure $ "val " <> parsedAlias <> " = " <> parsedQueryExpr

parseTableAlias :: Alias -> Either UnhandledError Text
parseTableAlias alias = case alias of
    Alias name Nothing -> parseName name
    other ->
        Left $ UnhandledError "Can't parse table alias" (T.pack $ ppShow other)


parseInsertStatement :: [Name] -> InsertSource -> Either UnhandledError Text
parseInsertStatement insertIntoNames insertSource = case insertSource of
    InsertQuery queryExpr ->
        printInsertQueryExpression insertIntoNames queryExpr
    blah -> Left $ UnhandledError "Can't handle insert source "
                                  (T.pack (ppShow blah))


scalaFormatTableName :: Text -> Text
scalaFormatTableName = T.map
    (\case
        '.'   -> '_'
        other -> other
    )


printInsertQueryExpression :: [Name] -> QueryExpr -> Either UnhandledError Text
printInsertQueryExpression insertIntoNames expression = case expression of
    Select setQuantifier selectList from sWhere groupBy having orderBy offset fetchFirst
        -> do
            parsedNames  <- parseNamesInsideIden insertIntoNames
            parsedSelect <- printSelectStatement setQuantifier
                                                 selectList
                                                 from
                                                 sWhere
                                                 groupBy
                                                 having
                                                 orderBy
                                                 offset
                                                 fetchFirst
            pure $ "val " <> parsedNames <> " = " <> parsedSelect
    With _ views queryExpression ->
        parseWithBlocks insertIntoNames views queryExpression
    other -> parseError "can't parse insert query" other

printQueryExpression :: QueryExpr -> Either UnhandledError Text
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
    other -> parseError "can't parse query expression" other

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
    -> Either UnhandledError Text
printSelectStatement setQuantifier selectList from sWhere groupBy having orderBy _ _
    = do
        fromClause  <- parseFromClause from
        whereClause <- case sWhere of
            Nothing   -> Right ""
            Just expr -> (\x -> "\n  .filter(" <> x <> ")")
                <$> parseScalarExpressionToAny expr
        groupByClause <- parseGroupingExpressions groupBy
        selectClause  <- parseSelectClause groupByClause selectList
        havingClause  <- case having of
            Nothing   -> Right ""
            Just expr -> (\x -> "\n  .filter(" <> x <> ")")
                <$> parseScalarExpressionToAny expr
        let sqClause = case setQuantifier of
                SQDefault -> ""
                All       -> ""
                Distinct  -> "\n  .distinct()"
        orderByClause <- case orderBy of
            [] -> Right ""
            _  -> (\x -> "\n  .orderBy(" <> x <> ")") <$> parseSortSpecs orderBy
        pure
            $  fromClause
            <> whereClause
            <> groupByClause
            <> selectClause
            <> orderByClause
            <> havingClause
            <> sqClause

parseFromClause :: [TableRef] -> Either UnhandledError Text
parseFromClause tableRefs = case tableRefs of
    []            -> parseError "empty FROM not supported" ""
    tableRef : xs -> do
        parsedHead  <- parseTableRef tableRef
        parsedTail1 <- traverse parseTableRef xs
        let parsedTail2 = fmap (\x -> "\n  .join(" <> x <> ")") parsedTail1
        pure $ parsedHead <> T.concat parsedTail2

parseTableRef :: TableRef -> Either UnhandledError Text
parseTableRef tableRef = case tableRef of
    TRJoin tableA isNatural joinType tableB maybeJoinCondition ->
        parseTableJoin tableA isNatural joinType tableB maybeJoinCondition
    TRSimple name        -> scalaFormatTableName <$> parseNamesInsideIden name
    TRAlias subRef alias -> do
        parsedSubRef <- parseTableRef subRef
        parsedAlias  <- parseAlias alias
        pure $ parsedSubRef <> ".as(\"" <> parsedAlias <> "\")"
    blah -> parseError "can't parse table ref" blah

parseTableJoin
    :: TableRef
    -> Bool
    -> JoinType
    -> TableRef
    -> Maybe JoinCondition
    -> Either UnhandledError Text
parseTableJoin tableA _ joinType tableB maybeJoinCondition = do
    parsedTableRefA     <- parseTableRef tableA
    parsedTableRefB     <- parseTableRef tableB
    parsedJoinCondition <- parseJoinCondition maybeJoinCondition
    let parsedJoinType = parseJoinType joinType
    pure
        $  parsedTableRefA
        <> "\n  ."
        <> joinKeyword
        <> "("
        <> parsedTableRefB
        <> parsedJoinCondition
        <> parsedJoinType
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

parseJoinCondition :: Maybe JoinCondition -> Either UnhandledError Text
parseJoinCondition Nothing              = Right ""
parseJoinCondition (Just joinCondition) = case joinCondition of
    JoinOn expr -> (", " <>) <$> parseScalarExpressionToAny expr
    JoinUsing names ->
        (\x -> ", Seq(" <> x <> ")") <$> parseNamesInUsingClause names

parseAlias :: Alias -> Either UnhandledError Text
parseAlias (Alias name _) = parseName name

parseSelectClause
    :: Text -> [(ScalarExpr, Maybe Name)] -> Either UnhandledError Text
parseSelectClause groupByClause selectList = case groupByClause of
    "" ->
        (\x -> "\n  .select(" <> T.intercalate ", " x <> ")")
            <$> traverse parseSelectList selectList
    _ -> (\x -> "\n  .agg(" <> T.intercalate "," x <> ")")
        <$> traverse parseSelectList (filter isAgg selectList)

isAgg :: (ScalarExpr, Maybe Name) -> Bool
isAgg (expr, _) = case expr of
    App _ _ -> True
    _       -> False


parseGroupingExpressions :: [GroupingExpr] -> Either UnhandledError Text
parseGroupingExpressions groupingExpressions = case groupingExpressions of
    [] -> Right ""
    other ->
        (\x -> "\n  .groupBy(" <> T.intercalate "," x <> ")")
            <$> traverse parseGroupingExpression other

parseGroupingExpression :: GroupingExpr -> Either UnhandledError Text
parseGroupingExpression groupingExpr = case groupingExpr of
    SimpleGroup scalarExpr -> parseScalarExpressionToCol scalarExpr
    other ->
        Left $ UnhandledError "parseGroupingExpr err" (T.pack $ ppShow other)

parseSelectList :: (ScalarExpr, Maybe Name) -> Either UnhandledError Text
parseSelectList (expr, alias) = do
    parsedAlias <- case alias of
        Just name ->
            (\x -> ".as(\"" <> x <> "\")") <$> parseNamesInsideIden [name]
        Nothing -> Right ""
    parsedScalarExpression <- parseScalarExpressionToCol expr
    pure $ parsedScalarExpression <> parsedAlias

parseScalarExpressionsGeneral :: [ScalarExpr] -> Either UnhandledError Text
parseScalarExpressionsGeneral exprs =
    T.intercalate ", " <$> traverse parseScalarExpressionToAny exprs


parseScalarExpressionsSecArgCol :: [ScalarExpr] -> Either UnhandledError Text
parseScalarExpressionsSecArgCol exprs =
    T.intercalate ", " <$> traverse parseScalarExpressionToCol exprs


parseScalarExpressionToCol :: ScalarExpr -> Either UnhandledError Text
parseScalarExpressionToCol expr = case expr of
    NumLit lit        -> Right $ "lit(" <> T.pack lit <> ")"
    StringLit _ _ lit -> Right $ "lit(\"" <> T.pack lit <> "\")"
    Iden name         -> parseIdenToCol name
    other             -> _parseScalarExpression other


parseScalarExpressionToAny :: ScalarExpr -> Either UnhandledError Text
parseScalarExpressionToAny expr = case expr of
    NumLit lit        -> Right $ T.pack lit
    StringLit _ _ lit -> Right $ "\"" <> T.pack lit <> "\""
    Iden name         -> parseIdenToAny name
    other             -> _parseScalarExpression other


parseScalarExpressionNoCol :: ScalarExpr -> Either UnhandledError Text
parseScalarExpressionNoCol expr = case expr of
    NumLit lit        -> Right $ T.pack lit
    StringLit _ _ lit -> Right $ "\"" <> T.pack lit <> "\""
    Iden name         -> parseIdenNoCol name
    other             -> _parseScalarExpression other

_parseScalarExpression :: ScalarExpr -> Either UnhandledError Text
_parseScalarExpression expr = case expr of
    Star                        -> Right "col(\"*\")"
    App  functionNames subExprs -> parseAppExpression functionNames subExprs
    Cast subExpr       typeName -> do
        parsedSubExpr <- parseScalarExpressionToCol subExpr
        parsedType    <- sparkType typeName
        pure $ parsedSubExpr <> ".cast(" <> parsedType <> ")"
    Case  test     whens     elsePart -> parseCaseExpression test whens elsePart
    BinOp subExpr1 funcNames subExpr2 -> parseBinOp subExpr1 funcNames subExpr2
    PostfixOp functionNames subExpr   -> do
        parsedSubExpr <- parseScalarExpressionToCol subExpr
        parsedFunctionNames <- parseFunctionNames functionNames
        pure $ parsedSubExpr <> parsedFunctionNames
    SpecialOp functionNames subExprs -> handleSpecialOp functionNames subExprs
    PrefixOp functionNames subExpr -> do
        parsedFunctionNames <- parseFunctionNames functionNames
        parsedSubExpr <- parseScalarExpressionToCol subExpr
        pure $ parsedFunctionNames <> parsedSubExpr
    Parens subExpr ->
        (\x -> "(" <> x <> ")") <$> parseScalarExpressionToCol subExpr
    HostParameter name _ -> Right $ T.pack name
    In inOrNotIn subExpr predValue ->
        parseInExpression inOrNotIn subExpr predValue
    WindowApp names args partition orderBy frame ->
        parseWindowFunction names args partition orderBy frame
    unparsed -> parseError "Can't parse expression in query block" unparsed

parseSortSpecs :: [SortSpec] -> Either UnhandledError Text
parseSortSpecs sortSpecs =
    T.intercalate "," <$> traverse parseSortSpec sortSpecs

-- Note that Spark uses opposite nulls order to Redshift/Psql to here we 
-- explicitly specify nulls first or nulls last when converting from psql
-- default
parseSortSpec :: SortSpec -> Either UnhandledError Text
parseSortSpec (SortSpec expr direction nullsOrder) =
    (<> directionQualifier) <$> parseScalarExpressionToCol expr
    where directionQualifier = case (direction, nullsOrder) of
            (DirDefault, NullsOrderDefault) -> ".asc_nulls_last()"
            (Asc, NullsOrderDefault) -> ".asc_nulls_last()"
            (Desc, NullsOrderDefault) -> ".desc_nulls_first()"
            (DirDefault, NullsFirst) -> ".asc_nulls_first()"
            (Asc, NullsFirst) -> ".asc_nulls_first()"
            (Desc, NullsFirst) -> ".desc_nulls_first()"
            (DirDefault, NullsLast) -> ".asc_nulls_last()"
            (Asc, NullsLast) -> ".asc_nulls_last()"
            (Desc, NullsLast) -> ".desc_nulls_last()"

parseWindowFunction
    :: [Name]
    -> [ScalarExpr]
    -> [ScalarExpr]
    -> [SortSpec]
    -> Maybe Frame
    -> Either UnhandledError Text
parseWindowFunction names _ partition orderBy _ = do
    parsedName      <- T.toLower <$> parseNamesInsideIden names
    parsedPartition <- parseScalarExpressionsGeneral partition
    parsedOrderBy   <- parseSortSpecs orderBy
    pure
        $  parsedName
        <> "().over(Window.partitionBy("
        <> parsedPartition
        <> ").orderBy("
        <> parsedOrderBy
        <> "))"

parseInExpression
    :: Bool -> ScalarExpr -> InPredValue -> Either UnhandledError Text
parseInExpression inOrNotIn expr predValue = do
    parsedExpression <- parseScalarExpressionToCol expr
    parsedPredicate  <- parseInPredicate predValue
    pure $ prefix <> parsedExpression <> ".isin(" <> parsedPredicate <> ")"
    where prefix = if inOrNotIn then "" else "!"

parseInPredicate :: InPredValue -> Either UnhandledError Text
parseInPredicate predValue = case predValue of
    InList      expr -> parseScalarExpressionsGeneral expr
    InQueryExpr expr -> printQueryExpression expr

parseBinOp :: ScalarExpr -> [Name] -> ScalarExpr -> Either UnhandledError Text
parseBinOp subExpr1 funcNames subExpr2 = do
    functionName <- parseFunctionNames funcNames
    parsedExpr1  <- parseScalarExpressionToCol subExpr1
    parsedExpr2  <- parseScalarExpressionToAny subExpr2
    case functionName of
        "not like" ->
            pure $ "!" <> parsedExpr1 <> ".like(" <> parsedExpr2 <> ")"
        "like" -> pure $ parsedExpr1 <> ".like(" <> parsedExpr2 <> ")"
        -- This is where the AST gets fucked up, thinks a schema dot separator is a function
        "." -> handleSpecialCaseForDotSeperator subExpr1 subExpr2
        _ -> pure $ parsedExpr1 <> " " <> functionName <> " " <> parsedExpr2

handleSpecialCaseForDotSeperator
    :: ScalarExpr -> ScalarExpr -> Either UnhandledError Text
handleSpecialCaseForDotSeperator expr1 expr2 = fmap
    (\x -> "col(\"" <> x <> "\")")
    parsedTableAndColumn
  where
    parsedTableAndColumn = case (expr1, expr2) of
        (Iden name1, Iden name2) -> do
            parsedName1 <- parseNamesInsideIden name1
            parsedName2 <- parseNamesInsideIden name2
            pure $ parsedName1 <> "." <> parsedName2
        (Iden name1, Star) -> (<> ".*") <$> parseNamesInsideIden name1
        other              -> parseError "parseDotSeparatorCase error" other


parseError :: Show a => Text -> a -> Either UnhandledError Text
parseError errorMessage expr =
    Left $ UnhandledError errorMessage (T.pack (ppShow expr))

parseIdenToCol :: [Name] -> Either UnhandledError Text
parseIdenToCol names = do
    parsedName <- parseNamesInsideIden names
    case parsedName of
        "TRUE"  -> pure "lit(true)"
        "FALSE" -> pure "lit(false)"
        "NULL"  -> pure "lit(null)"
        other   -> pure $ "col(\"" <> other <> "\")"

parseIdenToAny :: [Name] -> Either UnhandledError Text
parseIdenToAny names = do
    parsedName <- parseNamesInsideIden names
    case parsedName of
        "TRUE"  -> pure "true"
        "FALSE" -> pure "false"
        "NULL"  -> pure "null"
        other   -> pure $ "col(\"" <> other <> "\")"

parseIdenNoCol :: [Name] -> Either UnhandledError Text
parseIdenNoCol names = do
    parsedName <- parseNamesInUsingClause names
    case parsedName of
        "TRUE"  -> pure "true"
        "FALSE" -> pure "false"
        "NULL"  -> pure "null"
        other   -> pure other


parseAppExpression :: [Name] -> [ScalarExpr] -> Either UnhandledError Text
parseAppExpression functionNames subExpr = do
    functionName <- parseFunctionNames functionNames
    case functionName of
        "coalesce" -> (\x -> "coalesce(" <> x <> ")")
            <$> parseScalarExpressionsSecArgCol subExpr
        "greatest" -> (\x -> "greatest(" <> x <> ")")
            <$> parseScalarExpressionsSecArgCol subExpr
        "least" -> (\x -> "least(" <> x <> ")")
            <$> parseScalarExpressionsSecArgCol subExpr
        "date_part" -> parseDatePart subExpr
        "dateadd" -> parseDateAdd subExpr
        _           -> (\x -> functionName <> "(" <> x <> ")")
            <$> parseScalarExpressionsGeneral subExpr

parseDateAdd :: [ScalarExpr] -> Either UnhandledError Text
--parseDateAdd [expr1, expr2, expr3] = do
--    secondExpr <- parseScalarExpressionToAny expr2
--    if T.isInfixOf "col(" secondExpr then
--        pure $ parseDateAddAsExpr expr1 expr2 expr3
--    else
--        pure $ parseDateAddAsScala expr1 expr2 expr3
parseDateAdd other = parseError "dateadd expression not handled" other

parseDateAddAsExpr :: ScalarExpr -> ScalarExpr -> ScalarExpr -> Text
parseDateAddAsExpr expr1 expr2 expr3 = ""

parseDateAddAsScala :: ScalarExpr -> ScalarExpr -> ScalarExpr -> Text
parseDateAddAsScala expr1 expr2 expr3 = ""


parseDatePart :: [ScalarExpr] -> Either UnhandledError Text
parseDatePart [expr1, expr2] = do
    firstExpr  <- parseScalarExpressionNoCol expr1
    secondExpr <- parseScalarExpressionToCol expr2
    case firstExpr of
        "\"month\"" -> return $ "month(" <> secondExpr <> ")"
        "\"day\""   -> return $ "day(" <> secondExpr <> ")"
        "\"year\""  -> return $ "year(" <> secondExpr <> ")"
        _           -> parseError "Unrecongised date part " firstExpr
parseDatePart other = parseError "parseError" other

-- Currently used to handle between
handleSpecialOp :: [Name] -> [ScalarExpr] -> Either UnhandledError Text
handleSpecialOp functionNames exprs = case functionNames of
    [Name Nothing "between"] -> handleBetween exprs
    unparsed                 -> parseError "Couldn't handle special op" unparsed


handleBetween :: [ScalarExpr] -> Either UnhandledError Text
handleBetween exprs = case exprs of
    [expr1, expr2, expr3] -> do
        parsedExpr1 <- parseScalarExpressionToCol expr1
        parsedExpr2 <- parseScalarExpressionToAny expr2
        parsedExpr3 <- parseScalarExpressionToAny expr3
        return
            $  parsedExpr1
            <> ".between("
            <> parsedExpr2
            <> ", "
            <> parsedExpr3
            <> ")"
    other -> parseError ".between parse error" other


parseFunctionNames :: [Name] -> Either UnhandledError Text
parseFunctionNames names = case names of
    [name]             -> parseFunctionName name
    [schemaName, name] -> do
        parsedSchemaName   <- parseName schemaName
        parsedFunctionName <- parseFunctionName name
        return $ parsedSchemaName <> "." <> parsedFunctionName
    other -> parseError "Couldn't parse function name" other

parseFunctionName :: Name -> Either UnhandledError Text
parseFunctionName name = case name of
    Name Nothing subName -> Right $ sparkFunction $ T.pack subName
    other                -> parseError "Couldn't parse function name" other

parseNamesInUsingClause :: [Name] -> Either UnhandledError Text
parseNamesInUsingClause names = do
    parsedNames <- traverse parseName names
    let mappedNames = map (\x -> "\"" <> x <> "\"") parsedNames
    pure $ T.intercalate "," mappedNames

parseNamesInsideIden :: [Name] -> Either UnhandledError Text
parseNamesInsideIden names = case names of
    [name]             -> parseName name
    [schemaName, name] -> do
        parsedSchemaName <- parseName schemaName
        parsedName <- parseName name
        pure $ parsedSchemaName <> "." <> parsedName
    other              -> parseError "Couldn't parse name" other

parseName :: Name -> Either UnhandledError Text
parseName name = case name of
    Name Nothing subName -> Right $ T.pack subName
    other                -> parseError "Couldn't parse name" other

parseCaseExpression
    :: Maybe ScalarExpr
    -> [([ScalarExpr], ScalarExpr)]
    -> Maybe ScalarExpr
    -> Either UnhandledError Text
parseCaseExpression _ whens elsePart = do
    parsedWhens <- traverse parseWhen whens
    parsedElse  <- case elsePart of
        Nothing   -> Right ""
        Just expr -> (\x -> ".otherwise(" <> x <> ")")
            <$> parseScalarExpressionToAny expr
    return $ T.intercalate "." parsedWhens <> parsedElse
  where
    parseWhen (condExprs, thenExpr) = do
        parsedCondExpr <- parseScalarExpressionsGeneral condExprs
        parsedThenExpr <- parseScalarExpressionToAny thenExpr
        return $ "when(" <> parsedCondExpr <> ", " <> parsedThenExpr <> ")"

sparkFunction :: Text -> Text
sparkFunction function = case T.toLower function of
    "nvl"          -> "coalesce"
    "strpos"       -> "instr"
    "is null"      -> ".isNull"
    "is not null"  -> ".isNotNull"
    "="            -> "==="
    "<>"           -> "=!="
    "is true"      -> " === true"
    "is false"     -> " === false"
    "is not true"  -> " =!= true"
    "is not false" -> " =!= false"
    "not"          -> "!"
    other          -> T.toLower other

sparkType :: TypeName -> Either UnhandledError Text
sparkType typeName = case typeName of
    TypeName [Name Nothing "int8"] -> Right "DataTypes.LongType"
    TypeName [Name Nothing "date"] -> Right "DataTypes.DateType"
    TypeName [Name Nothing "integer"] -> Right "DataTypes.IntegerType"
    TypeName [name               ] -> parseError "Couldn't parse type" name
    PrecTypeName [Name Nothing "varchar"] _ -> Right "DataTypes.StringType"
    blah                           -> parseError "Couldn't parse type" blah
