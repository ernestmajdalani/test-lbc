## Lancer l'application
### Pre-requisites
Python version > v3.7
### Etapes
1. Accéder au répertoire du projet
```
cd path-to-project/test-lbc
```
2. Créer un environnement virtuel suivant `requirements.txt` et l'activer
```
python3 -m venv venv
source venv/bin/activate
```
3. Télécharger les librairies nécessaires:
```
python -m pip install -r requirements.txt
```
4. Préparer le déploiement et l'exécution de l'application
```
make build
cd dist
```
5. Lancer l'application spark à l'aide de `spark-submit`.
```
spark-submit --py-files jobs.zip,utils.zip --files config.json main.py --job pipeline_compact_graph pipeline_granular_graph --adhoc
```
**Paramètres**:
- `--job` (mandatory): job(s) a exécuter 
- `--ad-hoc` (optional): lance les fonctions `_adhoc` pour obtenir résultats de la partie "Traitement Ad-hoc" de l'exercice.

6. Observer les résultats 
   - Les fichiers de sortie des différents modèles de graphes de liaisons se trouvent dans le répertoire `dist/data/output/`.
   - Les résultats de la partie "Traitement Ad-hoc" sont imprimés au niveau du standard output.

## A propos des données
Nous disposons de 4 fichiers de données :
- **drugs.csv** contenant des noms de médicaments et leurs identifiants ATC Code maintenus par l'OMS.
   - On voit que les données sont complètes. 
   - Les drugs sont en majuscules, on pensera à les placer en minuscules pour les standardiser avec les titres 
d'articles de publications.
- **clinical_trials.csv** contenant des publications scientifiques d'essais cliniques. On note un identifiant et 
titre d'article, une date de publication du journal et le nom du journal.
  - `id` : on note une nomenclature spécifique debutant par "NCT" ce qui pourrait avoir une connotation scientifique particulière. 
Sur cette reflexion, on constate un sample pour lequel l'id est une chaine vide et on décide pour ce projet de la garder 
ainsi et de ne pas utiliser de fonction qui génère une chaine aléatoire.
  - `scientific_title` : on constate des charactères hexadecimals probablement dû à des erreurs d'encodage/décodage.
  - `date` : différents formats de dates notamment `dd month yyyy` et `dd/MM/yyyy`
  - `journal` : on constate également la présence de charactères hexadecimals
- **pubmed.(json|csv)** contenant des publications scientifiques du moteur de recherche PubMed. On note un 
identifiant et titre d'article, une date de publication du journal et le nom du journal.
  - `id` : on observe une séquence incrementale d'entiers. On se permettra donc de remplir les `id` manquant.  
  - `title` : pas d'anomalies observées
  - `date` : différents formats de dates notamment `dd/MM/yyyy` et `yyyy-MM-dd`
  - `journal` : pas d'anomalies observées

## Modélisation du graphe de liaison
On propose deux modèles pouvant représenter les liens entre une mention de `drug` et sa date, un article de type `clinical_trial`, 
un article de type `pubmed` et un `journal`.

### Granular Graph
Comme le nom le mentionne, on parle d'un modèle "granulaire" ou même "plat". Cette structure est avantageuse par le biais
de sa simplicité. Elle apporte la possibilité de partitionner les données par `date` ce qui est commun dans un environnement
Big Data. Le lien entre un `drug` et ses mentions respectives est établit par les champs suivants:
- `source_id`: est toujours l'ATC Code d'un médicament
- `source_name`: le nom du médicament
- `destination_id`: est l'`id` de l'article qui mentionne ce médicament
- `title`: le titre de l'article
- `date`: la date de cette mention
- `journal`: le journal dans lequel appartient l'article
- `edge_type`: indique le type de mention (ou relation) qu'il existe entre la source et destination e.g. pubmed ou clinical_trial

### Compact Graph
Ce modèle dispose d'une structure plus complexe et catégorise sous forme de listes d'objects les articles `pubmed`,
`clinical_trials` ainsi que `journal` qui mentionnent une `drug`. Il s'agit d'une structure qui évolue moins bien
à l'échelle mais qui satisfait les besoins de cet exercice. Contrairement au modèle "Granulaire", le partitionnement de 
ces données est restreint à l'ATC Code ou au nom du médicament. Nous avons :
- `atccode`: l'identifiant unique d'une drug
- `drug`: le nom d'une drug
- `pubmed`: liste d'articles PubMed qui mentionnent une drug avec leurs identifiants `id`, `title`, `date`
- `clinical_trials`: liste d'articles d'essais clinique qui mentionnent une drug avec leurs identifiants `id`, `title`, `date`
- `journal`: liste des journaux de publication et la date à laquelle un article mentionnant le médicament à été publié.

## Traitement

Afin de favoriser la réutilisation des différents modules ainsi que l'intégration des traitements dans un orchestrateur, 
on propose:
- de séparer les modules de traitements des différents objects `pubmed`, `clinical_trials` et `drug`. 
- chaque module contient des fonctions d'extraction `_extract_data`, de transformation `_transform_data` et d'execution
de ces dernières `run_job`.
- deux pipeline distinctes `pipeline_granular_graph` et `pipeline_compact_graph` pour créer les deux graphes de liaisons proposés ci-dessus.
Elles contiennent une phase d'extraction des différentes sources de données `_extract_data`, d'écriture `_load_data` sous format JSON 
et d'exécution de ces dernières `run_job`.
- un module `utils.common` contenant les fonctions qui peuvent être utilisées et partagées par différents modules du projets.
- une application `main.py` qui sert de point d'entrée à l'exécution de l'application. Elle permet d'énumérer les jobs
à exécuter à travers le paramètre `--job` ainsi que d'appeler les fonctions qui permettant de répondre à la partie "Traitement Adhoc"
de l'exercice; il suffit d'ajouter le paramètre `--ad-hoc`.

## Résultats
### Graphes de liaison
Les fichiers de sortie des différents modèles de graphes de liaisons se trouvent dans le répertoire `dist/data/output/`.

**Granular graph:**
```json
{
    "source_id": "R01AD",
    "source_name": "betamethasone",
    "destination_id": "10",
    "date": "2020-01-01",
    "journal": "the journal of maternal-fetal & neonatal medicine",
    "title": "clinical implications of umbilical artery doppler changes after betamethasone administration",
    "edge_type": "pubmed"
}
```
**Compact graph:**
```json
{
    "atccode": "V03AB",
    "drug": "ethanol",
    "pubmed": [
      {
        "id": "6",
        "title": "rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.",
        "date": "2020-01-01"
      }
    ],
    "clinical_trials": [],
    "journal": [
      {
        "title": "psychopharmacology",
        "date": "2020-01-01"
      }
    ]
}
```
### Traitement Ad-hoc
Cet exercice consiste à obtenir les journaux de publications qui mentionnent le plus de médicaments différents. 
On considère qu'un journal de publication est unique de par son nom et sa date de publication e.g. `('2020-01-01', 'psychopharmacology')`.

Deux approches ont été mise en place afin de répondre à cette partie :
- une approche `_adhoc_compact` qui traite le graphe de liaison `compact` sous forme de DataFrame PySpark.
- d'autre part, une approche `_adhoc_granular` qui traite le graphe de liaison `granular` à l'aide de la librairie `json`. 

Voici les résultats : 

<img src=screenshots/ad-hoc-results.png alt="Ad-hoc-result" width="600" height="500">

## Pour aller plus loin
On pourrait identifier plusieurs points pour considérer le passage à l'échelle de cette application. <br> 
Dans un premier temps, on pourrait envisager un déploiement vers le cloud afin de bénéficier de ressources de calculs 
conséquentes et "élastiques". On parlerait alors de "cluster sizing" pour allouer suffisament de mémoire, CPUs et 
d'espace disque adaptés à la volumétrie en question. Cela pourrait se faire par le biais d'un benchmark ainsi que la
mise en place de systèmes de Monitoring afin d'observer, comprendre et résoudre les problèmes liés à des éventuels 
délais de traitements. <br>
Dans un second temps, il serait nécessaire de configurer différents éléments propres à la configuration d'exécution de Spark comme par
exemple, les mémoires du driver et des exécuteurs e.g. `spark.executor.memory` ainsi que leur configuration de parallelisme 
e.g. `spark.executor.cores`, `spark.executor.instances`. Sur un plan fonctionnel, il serait nécessaire d'optimiser des 
opérations nécessitant des shuffle tels que les jointures et aggregations en équilibrant l'échange de partitions (data skewness) par le biais de clés adaptées ou même 
de paramètre tels que e.g. `spark.sql.shuffle.partitions`. Finalement, Spark est optimisé pour la manipulation de fichiers `parquet`.
Traiter des données sous ce format apporte de nombreux avantages de lecture e.g. pushdown predicate et d'écriture tels que le partitionnement et la compaction.


## SQL
### Query 1
```
SELECT `date`, SUM((prod_qty * prod_price)) AS ventes
FROM `TRANSACTION`
WHERE `date` >= '2020-01-01' AND `date` <= '2020-12-31'
GROUP BY `date`
```
### Query 2
```
SELECT T.client_id, SUM(case WHEN LOWER (P.prod_type) = 'meuble' then T.prod_price * T.prod_qty end) AS ventes_meubles, SUM(case WHEN LOWER(P.prod_type)='deco' THEN T.prod_price * T.prod_qty END) AS ventes_deco
FROM `TRANSACTION` as T
INNER JOIN PRODUCT_NOMENCLATURE AS P WHERE T.prod_id = P.prod_id
GROUP BY T.client_id
HAVING (T.date >= '2020-01-01' AND T.date <= '2020-12-31') AND (ventes_meubles IS NOT NULL OR ventes_deco IS NOT NULL);
```

