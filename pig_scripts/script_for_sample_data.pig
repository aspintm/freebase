-- nacitanie nasich tripletov
rows = LOAD '/freebase.gz' USING PigStorage('\t') AS (subject, predicate, object);

-- najdenie riadkov, so specifickym nazvom knihy
rows_by_name = FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/type.object.name>') AND (object == '"Fifty Shades of Grey"@en');

-- spojenie potrebnych dat pre konkretnu knihu
raw_result = JOIN rows BY subject, rows_by_name BY subject;

-- vygenerujem si vystup v pozadovanom tvare
result = FOREACH result GENERATE $1,$2,$3;

-- ulozenie vystupu do suboru
STORE result INTO '/sample_book_input.txt' USING PigStorage('\t');