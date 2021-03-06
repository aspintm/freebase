-- nacitame nase N triplets
rows = LOAD '/freebase.gz' USING PigStorage('\t') AS (subject, predicate, object);
-- rows = LOAD 'freebase_sample_book.txt' USING PigStorage('\t') AS (subject, predicate, object);

-- vyfiltrujeme si iba relevantne data
raw_books = FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/type.object.type>') AND (object == '<http://rdf.freebase.com/ns/book.written_work>');
raw_names = FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/type.object.name>') AND (object matches '.*@en');
raw_labels = FILTER rows BY (predicate == '<http://www.w3.org/2000/01/rdf-schema#label>') AND (object matches '.*@en');
raw_alts = FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/common.topic.alias>');
raw_characters = FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/book.book.characters>');
raw_authors =  FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/book.written_work.author>');

-- alternativne nazvy potrebujeme zgrupit na zaklade ich mid
-- {group, {raw_alts}}
alts_grouped = GROUP raw_alts BY subject;

-- autorov budeme taktiez zgrupovat, ich mid vsak potrebujeme prepojit s ich menami
-- {{raw_authors}, {raw_labels}}
authors_joined = JOIN
	raw_authors BY object,
	raw_labels BY subject;
-- {group, {authors_joined}}
authors_grouped = GROUP authors_joined BY raw_authors::subject;


-- postavy vytvorime analogicky k autorom
-- {{raw_characters}, {raw_labels}}
characters_joined = JOIN
	raw_characters BY object,
	raw_labels BY subject;
-- {group, {characters_joined}}
characters_grouped = GROUP characters_joined BY raw_characters::subject;

-- nakolko pig zatial nepodporuje LEFT OUTER JOIN viac ako 2 datasetov, potrebujeme si vytvorit docasne datasety, pre tento ucel
joined_names = JOIN
	raw_books BY subject LEFT OUTER,
	raw_names BY subject;
joined_labels = JOIN
	joined_names BY raw_books::subject LEFT OUTER,
	raw_labels BY subject;
joined_alts = JOIN
	joined_labels BY raw_books::subject LEFT OUTER,
	alts_grouped BY group;
joined_characters = JOIN
	joined_alts BY raw_books::subject LEFT OUTER,
	characters_grouped BY group;
joined_authors = JOIN
	joined_characters BY raw_books::subject LEFT OUTER,
	authors_grouped BY group;

-- {raw_books:0-s,1-p,2-o}, {raw_names:3-s,4-p,5-o}, {raw_labels:6-s,7-p,8-o}, {alts_grouped:9-group,10-raw_alts}, {characters_grouped:11-group,12-raw_characters}, {authors_grouped:13-group,14-raw_authors}
joined = DISTINCT joined_authors;

-- dataset result bude obsahovat strukturvany vystup
result = FOREACH joined {
	-- alternativne nazvy budu mat takuto strukturu "alts": [ {"lang": "en", "alt": "xxx"} ]
	alts = FOREACH $10 GENERATE
		REGEX_EXTRACT(object, '"[^@]*"@(.*)', 1) AS lang,
		REGEX_EXTRACT(object, '"([^@]*)"@.*', 1) AS alt;
	-- postavy a autori budu mat takuto strukturu "characters": [ {"character": "xxx"} ]
	characters = FOREACH $12 GENERATE FLATTEN(REGEX_EXTRACT(raw_labels::object, '"([^@]*)"@.*', 1)) AS character;
	authors = FOREACH $14 GENERATE FLATTEN(REGEX_EXTRACT(raw_labels::object, '"([^@]*)"@.*', 1)) AS author;
	GENERATE
		-- REGEX_EXTRACT($3, '"([^@]*)"@.*', 1) AS name,
		REGEX_EXTRACT($0, '^.*/(.*)>', 1) AS mid,
		REGEX_EXTRACT($8, '"([^@]*)"@.*', 1) AS title,
		alts AS alts,
		-- BagToString(characters, ',') AS characters,		BagToString by nam spravil vysledok v tvare "characters": "aaa, bbb"
		-- BagToString(authors, ',') AS authors;
		characters AS characters,
		authors AS authors;
};

-- ulozime si vysledok formatovany ako JSON
STORE result INTO '/freebase_books_output' USING JsonStorage();
