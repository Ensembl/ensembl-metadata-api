-- MySQL dump 10.13  Distrib 8.0.30, for Linux (x86_64)
--
-- Host: localhost    Database: ncbi_taxonomy
-- ------------------------------------------------------
-- Server version	8.0.30-0ubuntu0.22.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

DROP DATABASE IF EXISTS `test_ncbi_taxonomy`;
CREATE DATABASE /*!32312 IF NOT EXISTS*/ `test_ncbi_taxonomy` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

USE `test_ncbi_taxonomy`;

--
-- Table structure for table `ncbi_taxa_name`
--

DROP TABLE IF EXISTS `ncbi_taxa_name`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `ncbi_taxa_name` (
  `taxon_id` int unsigned NOT NULL,
  `name` varchar(255) NOT NULL,
  `name_class` varchar(50) NOT NULL,
  KEY `taxon_id` (`taxon_id`),
  KEY `name` (`name`),
  KEY `name_class` (`name_class`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ncbi_taxa_name`
--

LOCK TABLES `ncbi_taxa_name` WRITE;
/*!40000 ALTER TABLE `ncbi_taxa_name` DISABLE KEYS */;
INSERT INTO `ncbi_taxa_name` VALUES (1,'all','synonym'),(1,'root','scientific name'),(2,'Bacteria','scientific name'),(2,'bacteria','blast name'),(2,'eubacteria','genbank common name'),(2,'Monera','in-part'),(2,'Procaryotae','in-part'),(2,'Prokaryotae','in-part'),(2,'Prokaryota','in-part'),(2,'prokaryote','in-part'),(2,'prokaryotes','in-part'),(543,'Enterobacteraceae (ex Lapage 1979) Lapage 1982','authority'),(543,'Enterobacteraceae','synonym'),(543,'enterobacteria','blast name'),(543,'Enterobacteriaceae (ex Rahn 1937) Ewing et al. 1980','authority'),(543,'Enterobacteriaceae Rahn 1937 (Approved Lists 1980) emend. Adeolu et al. 2016','authority'),(543,'Enterobacteriaceae','scientific name'),(543,'gamma-3 proteobacteria','in-part'),(561,'Escherichia Castellani and Chalmers 1919','authority'),(561,'Escherichia','scientific name'),(562,'Achromobacter sp. ATCC 35328','includes'),(562,'ATCC 11775','type material'),(562,'\"Bacillus coli\" Migula 1895','authority'),(562,'Bacillus coli','synonym'),(562,'bacterium 10a','includes'),(562,'\"Bacterium coli commune\" Escherich 1885','authority'),(562,'Bacterium coli commune','synonym'),(562,'\"Bacterium coli\" (Migula 1895) Lehmann and Neumann 1896','authority'),(562,'Bacterium coli','synonym'),(562,'bacterium E3','includes'),(562,'CCUG 24','type material'),(562,'CCUG 29300','type material'),(562,'CIP 54.8','type material'),(562,'DSM 30083','type material'),(562,'E. coli','common name'),(562,'Enterococcus coli','synonym'),(562,'Escherichia coli (Migula 1895) Castellani and Chalmers 1919','authority'),(562,'Escherichia coli','scientific name'),(562,'Escherichia/Shigella coli','equivalent name'),(562,'Escherichia sp. 3_2_53FAA','includes'),(562,'Escherichia sp. MAR','includes'),(562,'IAM 12119','type material'),(562,'JCM 1649','type material'),(562,'LMG 2092','type material'),(562,'LMG:2092','type material'),(562,'NBRC 102203','type material'),(562,'NCCB 54008','type material'),(562,'NCTC 9001','type material'),(562,'strain U5/41','type material'),(1224,'Alphaproteobacteraeota Oren et al. 2015','authority'),(1224,'Alphaproteobacteraeota','synonym'),(1224,'Alphaproteobacteriota','synonym'),(1224,'proteobacteria','blast name'),(1224,'Proteobacteria [class] Stackebrandt et al. 1988','authority'),(1224,'Proteobacteria Garrity et al. 2005','authority'),(1224,'Proteobacteria','scientific name'),(1224,'purple bacteria and relatives','common name'),(1224,'purple bacteria','common name'),(1224,'purple non-sulfur bacteria','common name'),(1224,'purple photosynthetic bacteria and relatives','common name'),(1224,'purple photosynthetic bacteria','common name'),(1236,'Gammaproteobacteria Garrity et al. 2005 emend. Williams and Kelly 2013','authority'),(1236,'Gammaproteobacteria','scientific name'),(1236,'gamma proteobacteria','synonym'),(1236,'gamma subdivision','synonym'),(1236,'gamma subgroup','synonym'),(1236,'g-proteobacteria','blast name'),(1236,'Proteobacteria gamma subdivision','synonym'),(1236,'Purple bacteria, gamma subdivision','synonym'),(2759,'Eucarya','synonym'),(2759,'Eucaryotae','synonym'),(2759,'eucaryotes','genbank common name'),(2759,'Eukarya','synonym'),(2759,'Eukaryotae','synonym'),(2759,'Eukaryota','scientific name'),(2759,'eukaryotes','blast name'),(2759,'eukaryotes','common name'),(3193,'Embryophyta','scientific name'),(3193,'higher plants','common name'),(3193,'land plants','blast name'),(3193,'land plants','common name'),(3193,'plants','common name'),(3398,'Angiospermae','synonym'),(3398,'angiosperms','common name'),(3398,'flowering plants','blast name'),(3398,'flowering plants','genbank common name'),(3398,'Magnoliophyta','synonym'),(3398,'Magnoliopsida','scientific name'),(4447,'Liliopsida','scientific name'),(4447,'monocots','blast name'),(4447,'monocots','common name'),(4447,'Monocotyledoneae','synonym'),(4447,'monocotyledons','genbank common name'),(4479,'Bambusaceae Nakai, 1943','authority'),(4479,'Bambusaceae','synonym'),(4479,'Gramineae','synonym'),(4479,'grass family','common name'),(4479,'Poaceae Barnhart, 1895','authority'),(4479,'Poaceae','scientific name'),(4564,'Triticum L., 1753','authority'),(4564,'Triticum','scientific name'),(4565,'bread wheat','genbank common name'),(4565,'Canadian hard winter wheat','common name'),(4565,'common wheat','common name'),(4565,'Triticum aestivum L., 1753','authority'),(4565,'Triticum aestivum','scientific name'),(4565,'Triticum aestivum subsp. aestivum','synonym'),(4565,'Triticum vulgare','synonym'),(4565,'Triticum vulgare Vill., 1787','authority'),(4565,'wheat','common name'),(4734,'Commelinidae','synonym'),(4734,'commelinids','scientific name'),(4734,'Commeliniflorae','synonym'),(4751,'fungi','blast name'),(4751,'Fungi','scientific name'),(4751,'Mycota','synonym'),(4890,'ascomycete fungi','blast name'),(4890,'ascomycetes','genbank common name'),(4890,'Ascomycota','scientific name'),(4890,'sac fungi','common name'),(4891,'Hemiascomycetes','synonym'),(4891,'Saccharomycetes','scientific name'),(4892,'budding yeasts','blast name'),(4892,'Endomycetales','synonym'),(4892,'Saccharomycetales','scientific name'),(4893,'Eremotheciaceae','includes'),(4893,'Saccharomycetaceae','scientific name'),(4930,'Pachytichospora','synonym'),(4930,'Saccharomyces','scientific name'),(4932,'ATCC 18824','type material'),(4932,'baker\'s yeast','genbank common name'),(4932,'brewer\'s yeast','common name'),(4932,'Candida robusta','synonym'),(4932,'CBS 1171','type material'),(4932,'Mycoderma cerevisiae Desm., 1827','authority'),(4932,'Mycoderma cerevisiae','synonym'),(4932,'NRRL Y-12632','type material'),(4932,'Saccharomyces capensis','synonym'),(4932,'Saccharomyces cerevisiae (Desm.) Meyen, 1838','authority'),(4932,'Saccharomyces cerevisiae','scientific name'),(4932,'Saccharomyces italicus','synonym'),(4932,'Saccharomyces oviformis','synonym'),(4932,'Saccharomyces uvarum var. melibiosus','synonym'),(4932,'S. cerevisiae','common name'),(4932,'specimen-voucher:NRRL:Y:12632','type material'),(5794,'Apicomplexa Levine 1980, emend. Adl et al. 2005','authority'),(5794,'apicomplexans','blast name'),(5794,'apicomplexans','genbank common name'),(5794,'Apicomplexa','scientific name'),(5819,'Haemosporida','scientific name'),(5819,'haemosporidians','genbank common name'),(5819,'Haemosporina','synonym'),(5819,'Haemospororida','synonym'),(5820,'Plasmodium','scientific name'),(5833,'malaria parasite P. falciparum','genbank common name'),(5833,'Plasmodium falciparum','scientific name'),(5833,'Plasmodium (Laverania) falciparum','synonym'),(6072,'Eumetazoa','scientific name'),(6231,'Nemata','synonym'),(6231,'Nematoda','scientific name'),(6231,'nematode','common name'),(6231,'nematodes','blast name'),(6231,'nematodes','common name'),(6231,'roundworm','common name'),(6231,'roundworms','genbank common name'),(6236,'Rhabditida','scientific name'),(6237,'Caenorhabditis','scientific name'),(6239,'Caenorhabditis elegans (Maupas, 1900)','authority'),(6239,'Caenorhabditis elegans','scientific name'),(6239,'Rhabditis elegans Maupas, 1900','authority'),(6239,'Rhabditis elegans','synonym'),(6243,'Rhabditidae','scientific name'),(7711,'Chordata','scientific name'),(7711,'chordates','blast name'),(7711,'chordates','genbank common name'),(7742,'Vertebrata Cuvier, 1812','authority'),(7742,'Vertebrata','scientific name'),(7742,'vertebrates','blast name'),(7742,'vertebrates','genbank common name'),(7776,'Gnathostomata','scientific name'),(7776,'jawed vertebrates','genbank common name'),(8287,'Sarcopterygii','scientific name'),(9347,'eutherian mammals','common name'),(9347,'Eutheria','scientific name'),(9347,'Placentalia','synonym'),(9347,'placental mammals','common name'),(9347,'placentals','blast name'),(9347,'placentals','genbank common name'),(9443,'Primata','synonym'),(9443,'primate','equivalent name'),(9443,'primates','blast name'),(9443,'Primates Linnaeus, 1758','authority'),(9443,'Primates','scientific name'),(9526,'Catarrhini','scientific name'),(9604,'great apes','genbank common name'),(9604,'Hominidae Gray, 1825','authority'),(9604,'Hominidae','scientific name'),(9604,'Pongidae','synonym'),(9605,'Homo Linnaeus, 1758','authority'),(9605,'Homo','scientific name'),(9605,'humans','common name'),(9606,'Homo sapiens Linnaeus, 1758','authority'),(9606,'Homo sapiens','scientific name'),(9606,'human','genbank common name'),(32523,'Tetrapoda','scientific name'),(32523,'tetrapods','genbank common name'),(32524,'Amniota','scientific name'),(32524,'amniotes','genbank common name'),(32525,'Theria Parker & Haswell, 1897','authority'),(32525,'Theria','scientific name'),(33090,'Chlorobionta Jeffrey, 1982','authority'),(33090,'Chlorobionta','synonym'),(33090,'Chlorophyta/Embryophyta group','equivalent name'),(33090,'chlorophyte/embryophyte group','equivalent name'),(33090,'Chloroplastida Adl et al. 2005','authority'),(33090,'Chloroplastida','synonym'),(33090,'green plants','blast name'),(33090,'green plants','common name'),(33090,'Viridiplantae Cavalier-Smith, 1981','authority'),(33090,'Viridiplantae','scientific name'),(33154,'Fungi/Metazoa group','synonym'),(33154,'Opisthokonta Cavalier-Smith 1987','authority'),(33154,'Opisthokonta','scientific name'),(33154,'opisthokonts','synonym'),(33208,'Animalia','synonym'),(33208,'animals','blast name'),(33208,'metazoans','genbank common name'),(33208,'Metazoa','scientific name'),(33208,'multicellular animals','common name'),(33213,'Bilateria','scientific name'),(33317,'Protostomia','scientific name'),(33511,'deuterostomes','common name'),(33511,'Deuterostomia','scientific name'),(33630,'Alveolata','scientific name'),(33630,'alveolates','genbank common name'),(35493,'Streptophyta Bremer, 1985','authority'),(35493,'Streptophyta','scientific name'),(36329,'Plasmodium falciparum 3D7','scientific name'),(36329,'Plasmodium falciparum (isolate 3D7)','synonym'),(38820,'Cyperales','includes'),(38820,'Poales','scientific name'),(38820,'Typhales','includes'),(40674,'Mammalia','scientific name'),(40674,'mammals','blast name'),(40674,'mammals','genbank common name'),(55879,'Rhabditoidea','scientific name'),(55885,'Peloderinae','scientific name'),(58023,'Tracheophyta','scientific name'),(58023,'Tracheophyta Sinnott ex Cavalier-Smith, 1998','authority'),(58023,'vascular plants','blast name'),(58023,'vascular plants','common name'),(58024,'seed plants','blast name'),(58024,'seed plants','common name'),(58024,'Spermatophyta','scientific name'),(78536,'Euphyllophyta','scientific name'),(78536,'euphyllophytes','equivalent name'),(83333,'Escherichia coli K12','equivalent name'),(83333,'Escherichia coli K-12','scientific name'),(89593,'Craniata','scientific name'),(91347,'Enterobacterales Adeolu et al. 2016','authority'),(91347,'Enterobacterales','scientific name'),(91347,'Enterobacteriaceae and related endosymbionts','synonym'),(91347,'Enterobacteriaceae group','synonym'),(91347,'Enterobacteriales','synonym'),(91347,'gamma-3 proteobacteria','in-part'),(117570,'Teleostomi','scientific name'),(117571,'bony vertebrates','genbank common name'),(117571,'Euteleostomi','scientific name'),(119089,'Adenophorea','synonym'),(119089,'Chromadorea','scientific name'),(131221,'Charophyta/Embryophyta group','synonym'),(131221,'charophyte/embryophyte group','equivalent name'),(131221,'Streptophytina','scientific name'),(131567,'biota','synonym'),(131567,'cellular organisms','scientific name'),(147368,'Pooideae','scientific name'),(147389,'Triticeae Dumort., 1824','authority'),(147389,'Triticeae','scientific name'),(147537,'Saccharomycotina','scientific name'),(147537,'true yeasts','genbank common name'),(207598,'Homininae','scientific name'),(207598,'Homo/Pan/Gorilla group','synonym'),(314146,'Euarchontoglires','scientific name'),(314293,'Anthropoidea','synonym'),(314293,'Simiiformes','scientific name'),(314295,'ape','common name'),(314295,'apes','genbank common name'),(314295,'Hominoidea','scientific name'),(359160,'BEP clade','equivalent name'),(359160,'BOP clade','scientific name'),(376913,'Haplorrhini','scientific name'),(418107,'Laverania Bray 1958','authority'),(418107,'Laverania','synonym'),(418107,'Plasmodium (Laverania)','scientific name'),(422676,'Aconoidasida Mehlhorn et al. 1980','authority'),(422676,'Aconoidasida','scientific name'),(422676,'Hematozoa Vivier 1982','synonym'),(451864,'Dikarya','scientific name'),(511145,'Escherichia coli MG1655','synonym'),(511145,'Escherichia coli strain MG1655','equivalent name'),(511145,'Escherichia coli str. K12 substr. MG1655','equivalent name'),(511145,'Escherichia coli str. K-12 substr. MG1655','scientific name'),(511145,'Escherichia coli str. MG1655','equivalent name'),(559292,'Saccharomyces cerevisiae S288C','scientific name'),(716545,'saccharomyceta','scientific name'),(1206794,'Ecdysozoa','scientific name'),(1338369,'Dipnotetrapodomorpha','scientific name'),(1437010,'Boreoeutheria','scientific name'),(1437010,'Boreotheria','synonym'),(1437183,'Mesangiospermae M.J.Donoghue, J.A.Doyle & P.D.Cantino, 2007','authority'),(1437183,'Mesangiospermae','scientific name'),(1437197,'Petrosaviidae','scientific name'),(1437197,'Petrosaviidae S.W.Graham & W.S.Judd, 2007','authority'),(1639119,'Plasmodiidae Mesnil, 1903','authority'),(1639119,'Plasmodiidae','scientific name'),(1648030,'Triticinae Fr., 1835','authority'),(1648030,'Triticinae','scientific name'),(1648038,'Triticodae','scientific name'),(1648038,'Triticodae T.D. Macfarl. & L.Watson, 1982','authority'),(2301116,'Rhabditina','scientific name'),(2301119,'Rhabditomorpha','scientific name'),(2698737,'Sar Burki et al. 2008','authority'),(2698737,'Sar','scientific name'),(2698737,'SAR supergroup','synonym'),(38820,'4478','merged_taxon_id'),(38820,'4727','merged_taxon_id'),(119089,'27837','merged_taxon_id'),(6236,'33251','merged_taxon_id'),(4930,'36915','merged_taxon_id'),(4565,'39424','merged_taxon_id'),(117571,'40673','merged_taxon_id'),(4893,'44280','merged_taxon_id'),(6243,'54603','merged_taxon_id'),(4893,'221665','merged_taxon_id'),(4565,'235075','merged_taxon_id'),(562,'469598','merged_taxon_id'),(562,'662101','merged_taxon_id'),(562,'662104','merged_taxon_id'),(562,'1637691','merged_taxon_id'),(4479,'1661618','merged_taxon_id'),(562,'1806490','merged_taxon_id'),(1,'2022-05-11 11:21:23','import date');
/*!40000 ALTER TABLE `ncbi_taxa_name` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ncbi_taxa_node`
--

DROP TABLE IF EXISTS `ncbi_taxa_node`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8 */;
CREATE TABLE `ncbi_taxa_node` (
  `taxon_id` int unsigned NOT NULL,
  `parent_id` int unsigned NOT NULL,
  `rank` char(32) NOT NULL DEFAULT '',
  `genbank_hidden_flag` tinyint(1) NOT NULL DEFAULT '0',
  `left_index` int NOT NULL DEFAULT '0',
  `right_index` int NOT NULL DEFAULT '0',
  `root_id` int NOT NULL DEFAULT '1',
  PRIMARY KEY (`taxon_id`),
  KEY `parent_id` (`parent_id`),
  KEY `rank` (`rank`),
  KEY `left_index` (`left_index`),
  KEY `right_index` (`right_index`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ncbi_taxa_node`
--

LOCK TABLES `ncbi_taxa_node` WRITE;
/*!40000 ALTER TABLE `ncbi_taxa_node` DISABLE KEYS */;
INSERT INTO `ncbi_taxa_node` VALUES (1,0,'no rank',0,1,4839748,1),(2,131567,'superkingdom',0,454785,1518028,1),(543,91347,'family',0,527109,572226,1),(561,543,'genus',0,540630,549495,1),(562,561,'species',1,540631,547386,1),(1224,2,'phylum',0,454786,907829,1),(1236,1224,'class',0,454787,699792,1),(2759,131567,'superkingdom',0,1546141,4803280,1),(3193,131221,'clade',0,1592311,2050748,1),(3398,58024,'class',0,1612133,2028338,1),(4447,1437183,'clade',0,1612859,1707618,1),(4479,38820,'family',0,1627987,1648550,1),(4564,1648030,'genus',0,1646777,1646996,1),(4565,4564,'species',1,1646778,1646809,1),(4734,1437197,'clade',1,1622243,1667602,1),(4751,33154,'kingdom',0,2053415,2416594,1),(4890,451864,'phylum',0,2093309,2306878,1),(4891,147537,'class',0,2100390,2112985,1),(4892,4891,'order',0,2100391,2112760,1),(4893,4892,'family',0,2100392,2102505,1),(4930,4893,'genus',0,2100505,2101672,1),(4932,4930,'species',1,2100528,2101173,1),(5794,33630,'phylum',0,4739114,4757307,1),(5819,422676,'order',0,4739198,4750405,1),(5820,1639119,'genus',0,4739270,4743733,1),(5833,418107,'species',1,4743222,4743567,1),(6072,33208,'clade',1,2428610,4709233,1),(6231,1206794,'phylum',0,2456730,2492541,1),(6236,119089,'order',0,2464694,2484259,1),(6237,55885,'genus',0,2483022,2483179,1),(6239,6237,'species',1,2483025,2483026,1),(6243,55879,'family',0,2483020,2484177,1),(7711,33511,'phylum',0,4480907,4707468,1),(7742,89593,'clade',0,4483205,4707428,1),(7776,7742,'clade',1,4483206,4706229,1),(8287,117571,'superclass',1,4591507,4706226,1),(9347,32525,'clade',0,4681311,4706198,1),(9443,314146,'order',0,4693344,4695391,1),(9526,314293,'parvorder',0,4694543,4695346,1),(9604,314295,'family',0,4695265,4695344,1),(9605,207598,'genus',0,4695299,4695316,1),(9606,9605,'species',1,4695300,4695305,1),(32523,1338369,'clade',1,4591583,4706224,1),(32524,32523,'clade',1,4616682,4706223,1),(32525,40674,'clade',1,4679940,4706199,1),(33090,2759,'kingdom',0,1565736,2053413,1),(33154,2759,'clade',1,2053414,4709947,1),(33208,33154,'kingdom',0,2416905,4709246,1),(33213,6072,'clade',1,2456723,4709232,1),(33317,33213,'clade',1,2456724,4467859,1),(33511,33213,'clade',1,4467860,4707861,1),(33630,2698737,'clade',0,4728125,4767506,1),(35493,33090,'phylum',0,1591573,2053328,1),(36329,5833,'isolate',1,4743253,4743254,1),(38820,4734,'order',0,1627986,1663273,1),(40674,32524,'class',0,4679913,4706222,1),(55879,2301119,'superfamily',0,2483019,2484224,1),(55885,6243,'subfamily',0,2483021,2483252,1),(58023,3193,'clade',0,1612130,2050747,1),(58024,78536,'clade',0,1612132,2031861,1),(78536,58023,'clade',1,1612131,2048940,1),(83333,562,'strain',1,540634,540705,1),(89593,7711,'subphylum',0,4483204,4707429,1),(91347,1236,'order',0,527108,589837,1),(117570,7776,'clade',1,4487755,4706228,1),(117571,117570,'clade',0,4487756,4706227,1),(119089,6231,'class',0,2464693,2492514,1),(131221,35493,'subphylum',1,1592310,2053319,1),(131567,1,'no rank',1,454784,4803281,1),(147368,359160,'subfamily',0,1641279,1648496,1),(147389,1648038,'tribe',0,1645725,1647156,1),(147537,716545,'subphylum',0,2100389,2112998,1),(207598,9604,'subfamily',1,4695266,4695317,1),(314146,1437010,'superorder',0,4693269,4706168,1),(314293,376913,'infraorder',1,4693878,4695347,1),(314295,9526,'superfamily',1,4695160,4695345,1),(359160,4479,'clade',0,1637546,1648497,1),(376913,9443,'suborder',0,4693877,4695390,1),(418107,5820,'subgenus',0,4743221,4743592,1),(422676,5794,'class',0,4739197,4752236,1),(451864,4751,'subkingdom',0,2093308,2416587,1),(511145,83333,'no rank',1,540639,540640,1),(559292,4932,'strain',1,2100559,2100560,1),(716545,4890,'clade',1,2100388,2306877,1),(1206794,33317,'clade',0,2456729,4301244,1),(1338369,8287,'clade',1,4591520,4706225,1),(1437010,9347,'clade',1,4682028,4706173,1),(1437183,3398,'clade',1,1612858,2028325,1),(1437197,4447,'subclass',1,1618614,1707617,1),(1639119,5819,'family',0,4739269,4743838,1),(1648030,147389,'subtribe',0,1646686,1647105,1),(1648038,147368,'no rank',0,1645724,1647169,1),(2301116,6236,'suborder',0,2481947,2484234,1),(2301119,2301116,'infraorder',0,2483018,2484233,1),(2698737,2759,'clade',0,4728124,4803255,1);
/*!40000 ALTER TABLE `ncbi_taxa_node` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-10-30  9:14:23