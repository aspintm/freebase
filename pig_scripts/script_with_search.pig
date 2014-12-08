-- nacitame nase N triplets

-- pomocou parametrov a pridat popis ku tym parametrom, pricom ten moj search dat do samostatneho skriptu
-- ono to ma vyplut jeden velky vystup so vsetkymi knihami, plus este mozem tam dat rok, pripadne vydavatelstvo a nad nimi statistiky robit
rows = LOAD 'freebase.gz' USING PigStorage('\t') AS (subject, predicate, object);

-- vyfiltrujeme si iba relevantne data
raw_books = FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/type.object.type>') AND (object == '<http://rdf.freebase.com/ns/book.written_work>');
raw_names = FILTER rows BY (predicate == '<http://rdf.freebase.com/ns/type.object.name>');
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
joined_by_mid = JOIN
	raw_books BY subject,
	raw_names BY subject,
	raw_labels BY subject;
joined_by_mid_alts = JOIN
	joined_by_mid BY raw_books::subject LEFT OUTER,
	alts_grouped BY group;
joined_by_mid_alts_characters = JOIN
	joined_by_mid_alts BY raw_books::subject LEFT OUTER,
	characters_grouped BY group;
joined_by_mid_alts_characters_authors = JOIN
	joined_by_mid_alts_characters BY raw_books::subject LEFT OUTER,
	authors_grouped BY group;

-- nacitame si zoznam hladanych mien v anglickom jazyku
names = LOAD 'freebase_names_of_books.txt' USING PigStorage() AS (name);
names = FOREACH names GENERATE CONCAT(name, '@en') AS name;

-- dataset joined bude obsahovat vsetky potrebne data v nestrukturovanej podobe
-- {{names:0-name}, {raw_books:1-s,2-p,3-o}, {raw_names:4-s,5-p,6-o}, {raw_titles:7-s,8-p,9-o}, {alts_grouped:10-group,11-raw_alts}, {characters_grouped:12-group,13-raw_characters}, {authors_grouped:14-group,15-raw_authors}}
joined = JOIN names BY name LEFT OUTER, joined_by_mid_alts_characters_authors BY raw_names::object;
joined = DISTINCT joined;

-- dataset result bude obsahovat strukturvany vystup
result = FOREACH joined {
	-- alternativne nazvy budu mat takuto strukturu "alts": [ {"lang": "en", "alt": "xxx"} ]
	alts = FOREACH $11 GENERATE
		REGEX_EXTRACT(object, '"[^@]*"@(.*)', 1) AS lang,
		REGEX_EXTRACT(object, '"([^@]*)"@.*', 1) AS alt;
	-- postavy a autori budu mat takuto strukturu "characters": [ {"character": "xxx"} ]
	characters = FOREACH $13 GENERATE FLATTEN(REGEX_EXTRACT(raw_labels::object, '"([^@]*)"@.*', 1)) AS character;
	authors = FOREACH $15 GENERATE FLATTEN(REGEX_EXTRACT(raw_labels::object, '"([^@]*)"@.*', 1)) AS author;
	GENERATE
		-- REGEX_EXTRACT($0, '"([^@]*)"@.*', 1) AS name,
		REGEX_EXTRACT($1, '^.*/(.*)>', 1) AS mid,
		REGEX_EXTRACT($9, '"([^@]*)"@.*', 1) AS title,
		alts AS alts,
		-- BagToString(characters, ',') AS characters,		BagToString by nam spravil vysledok v tvare "characters": "aaa, bbb"
		-- BagToString(authors, ',') AS authors;
		characters AS characters,
		authors AS authors;
};

-- ulozime si vysledok formatovany ako JSON
STORE result INTO 'freebase_books_output' USING JsonStorage();
