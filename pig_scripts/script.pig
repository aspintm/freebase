-- nacitanie nasich tripletov
rows = LOAD '/freebase.gz' USING PigStorage('\t') AS (subject, predicate, object);

-- najdenie riadkov, so specifickym nazvom knihy
rows_by_name = FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/type.object.name>') AND (object == '"Fifty Shades of Grey"@en');

-- najdenie riadkov, ktore nam urcuju label
rows_by_label = FILTER rows BY (predicate == '<http://www.w3.org/2000/01/rdf-schema#label>');

-- najdenie riadkov, ktore nam urcuju alternativne nazvy knih
rows_by_alias = FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/common.topic.alias>');

-- spojenie potrebnych dat pre konkretnu knihu
-- prve 3 stlpce su vyfiltrovane podla mena, dalsie 3 podla label a dalsie 3 podla alias
raw_rows_joined = JOIN rows_by_name BY subject, rows_by_label BY subject, rows_by_alias BY subject;

-- prevediem si ziskane data do formatu mid, label, alias
rows_joined = FOREACH raw_rows_joined GENERATE $0 AS mid, $5 AS label, $8 AS alias;

-- prevedenie label z formatu "aaa"@en do citatelneho formatu: { "lang", "label" }
rows_joined = FOREACH rows_joined GENERATE mid, {REGEX_EXTRACT(label, '([^@]*)@(.*)', 2), REGEX_EXTRACT(label, '([^@]*)@(.*)', 1)} AS label, alias;

-- prevedenie alts z formatu "aaa"@en do citatelneho formatu: { "lang", "alias" }
rows_joined = FOREACH rows_joined GENERATE mid, label, {REGEX_EXTRACT(alias, '([^@]*)@(.*)', 2), REGEX_EXTRACT(alias, '([^@]*)@(.*)', 1)} AS alias;

-- ulozenie vystupu do suboru v JSON formate
STORE rows_joined INTO '/book_output.txt' USING JsonStorage();

-- hdfs dfs -cat /sample_book_vystup.txt/part-m-00000
