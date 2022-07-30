# TIW6 : Compte rendu TP Spark Partitionnement


#### CLEMENT Florent
#### CONTRERAS Baptiste


## Découpage en deux parties égales
Nous avons deux fonctions :

- `partionne_en_deux` : qui appelle `partionne_en_deux_Rec` pour découper en deux partitions de même taille.
- `partionne_en_deux_Rec` qui permet de découper en deux partitions de taille nb_X et nb_Y

Concernant l'implémentation, nous avons suivi la description proposée dans l'énoncé. La seule différence apportée est que nous avons choisi de regrouper les éléments supérieurs et égaux au pivot.

## Généralisation

Nous avons implémenté cette partie dans la fonction `partionne_en_N_Rec` de la classe `PartitionN`.
Nous avons choisi de réutiliser la fonction `partionne_en_deux_Rec`. L'idée derrière notre implémentation est la suivante :

- Si N est pair, on découpe en deux parties égales puis chaque nouvelle partie obtenue est découpée en N/2. Le découpage de ces deux parties sera ensuite assemblé.
- Si N est impair, on divise en deux parties, une petite de taille **RddSize / N** et une grosse en **(N-1)RddSize / N**. La grosse partie obtenue est découpée en N-1 parties, qui sont assemblées à la petite partie

(Nous avons mis un exemple dans le commentaire de la fonction)

## Index
Le code est présent dans le main de la class `Index`
Nous n'avons pas pu faire de test unitaire pour cette partie car nous avons un bug lors de l'écriture des partitions sous windows. Nous avons donc directement importé un jeu de données de 56 lignes sur l'hdfs pour tester plus facilement
## Requête de zone
Le code est présent dans le main de la class `Requetage`
