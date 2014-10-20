-- nacitanie nasich tripletov
raw_rows = LOAD '/sample_books_vstup.tar.gz' USING PigStorage('\t') AS (subject, predicate, object);

-- najdenie riadkov, so specifickym nazvom knihy
rows_by_name = FILTER raw_rows BY (predicate == '<http://rdf.freebase.com/ns/type.object.name>') AND (object == '"The Soul Toss"@en');

-- najdenie riadkov, ktore nam urcuju alternativne nazvy knih
rows_by_type = FILTER raw_rows BY (predicate == '<http://rdf.freebase.com/ns/common.topic.alias>');

-- najdenie riadkov, ktore pozostavaju z konkretneho mid pre zadany objekt a obsahuju predikat alias
rows_by_name_and_type = JOIN raw_rows BY subject LEFT OUTER, rows_by_name BY subject;

-- extrakcia aliasov v surovom stave ("aaa"@en)
raw_alts = FOREACH rows_by_name_and_type GENERATE ($2);

-- prevedenie aliasov do citatelneho formatu: { "lang", "alias" }
alts = FOREACH raw_alts GENERATE {REGEX_EXTRACT($0, '([^@]*)@(.*)', 2), REGEX_EXTRACT($0, '([^@]*)@(.*)', 1)};

-- ulozenie vystupu do vystupneho suboru
STORE alts INTO 'sample_books_vystup.txt' USING PigStorage();
