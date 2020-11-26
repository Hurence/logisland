# But du processeur

Ce processeur met à jours els sessions web correspondant auw web events qu'il reçoit. 

# Description du comportement actuelle

Actuellement le processeur fonctionne comme ça :

* Pour chaque session concerné par les events en entréé, on fait un multiget sur le mapping pour obtenir la dernière session (les sessions #n).
* Ensuite on fait un multi get pour obtenir chacune de ces sessions
* Ensuite on applique les évènements en entrée à ces sessions.
* On sauvegarde les évènements en entrée avec leur sessions associé (potentiellement modifié avec #n)
* On renvoie les nouvelles sessions qui ont été créer/modifier par le traitement des nouveaux évènements.

Voir schéma IncrementalWebSession_current_archi.plantuml

## Problème

Avant chaque requête ES il faut faire un refresh des index concernées pour être sur d'avoir les informations à jour.
En effet faire un bulkFlush ne garantie pas d'avoir des données à jour lors d'une prochaine requête.
Il n y a que le refresh qui donne cette garantie (preuve par tests d'intégration IncrementalWebSessionIT). 

# Proposition d'une nouvelle architecture

## Hypothèses

On suppose que le moteur est kafka stream. Et que les données d'un même utilisateur sont toujours données dans une même partition kafka.

Dans ce cas la, Structured stream avec kafka garantie que les données provenant d'une même partition kafka est toujours exécuté par
le même exécuteur. Ce qui veut dire que l'on peut utiliser un service de cache qui sera donc toujours synchrone au sein d'un
exécuteur donnée.

On suppose que dans kafka on a exclusivement des données chronologique au niveau de chaque user.
C'est à dire que pour un partiId donnée, on ne peut pas avoir eventN+1 avec un timestamp inférieur à eventN.


## Solution pour la performance

Dans ce cas la on peut utiliser un cache pour stoquer le mapping d'une sessions à une sub sessions de
manière robuste et performante (sans requête à ES). De même en ce qui concerne les données d'une session en particulier.

Il reste ensuite à trouver un moyen de gérer un rewind ET un restart sans rewind du job mais en ayant déjà des données précédentes dans ES. De manière que cela
n'est pas d'impacte. Il faut que le job soit idempotent.

## Proposition pour simplifier le flux

Changer la session id a l'entrée de logisland ou à la sortie de divolte. En effet la session fournie par divolte ne devrait pas être
la même sur deux journées différentes. Du coup nous pouvons modifier la session de la manière suivante pour garantir cela :

sessionId = divolt_session + yyyy-MM-dd (attention au timezone ! local ou UTC ?)

De cette manière nous n'avons pas besoin de gérer le changement de session lorsqu'il ya un changement de jour !
De plus pour savoir si il y a une sub sessions nous n'avons qu'a chercher les sessions dans l'index au niveau de la journée correspondante !
En effet sinon la dernière sub session pourrait très bien daté d'il y a 3 mois (voir plus dans un cas extrême !)

## Solution pour l'idempotence

Il y a deux cas :
1) Soit le cache connait la sessions demandé et/ou le mapping de la session demandé.
2) Soit le cache ne connait pas une sessions demandé et/ou son mapping.

Dans le premier cas il n y a pas de problème et le résultat d'un même processing sera toujours le même.
La difficulté est donc de savoir comment on alimente/initialise le cache lorsqu'une information est inconnue.

Je propose que l'on élimine l'index de mapping des sessions. En effet celui-là pose beaucoup de problème pour l'idempotence.
De plus c'est un index qui n'a aucune utilité en dehors de la mécanique interne du job analytique.

Nous devons trouver une solution à ces deux problèmes :
* obtenir l'information d'une session lorsqu'elle n'est pas présente dans le cache
* obtenir la dernière sous sessions d'une session données

### obtenir l'information d'une session lorsqu'elle n'est pas présente dans le cache

Comme le session id est capé à une journée on peut directement chercher cette session
 dans l'index correspondant, actuellement on a un index par mois donc l'index du mois correspondant à l'évènement. 

### obtenir la dernière sous sessions d'une session données

Comme le session id est capé à une journée on peut directement chercher cette session
 dans l'index correspondant, actuellement on a un index par mois donc l'index du mois correspondant à l'évènement.
Il faut trouver une query qui retourne toutes les sub sessions pour une session donnée et trier par le timestamp
afin d'obtenir la dernière.

### Cas spécifique du rewind
 
Le problème est qu'on va avoir les données de la session qui a déjà traité les évènements que l'on a en entrés.
De plus les évènements que nous avons sont potentiellement par exemple uniquement les deux derniers évènements de la sub session 2.
Il faut donc reprendre cette sessions et toute les sessions suivantes. Et pour reconstituer la sub sessions 2 nous allons devoir nous assurer
d'avoir tous les évènements la concernant.

En effet sinon des paramètres de la session comme "eventCounter" seront erronés.
Il faut donc pour moi :

1. Détecter que l'on est dans un cas de rewind 
2. Dans ce cas effacer les toutes les sessions du futur (par rapport aux évènements en input). A noter que de toute manière nous
avons toujours la donnée brute a disposition dans l'index des évènements et dans kafka.
3. Récupérer tous les évènements des users pour la/les journées par rapport aux évènements d'entrées.
4. Reprocesser les sessions

---
**NOTE** : On pourrait se dire que l'étape deux n'est pas nécessaire car les sessions vont être écraser par les nouvelles recalculés.
Seulement si on change la logique des changements de sessions cela pourrait être faux.

---

---
**NOTE** : Pour les events ou a pas ce problèmes car les nouveaux évènements vont écrasés les anciens sans effet de bord a ma connaissance.

---

#### Détecter que l'on est dans un cas de rewind

A partir de chaque event en entrée on trouve pour chaque partyId le T-event-min (timestamp minimum).
pour chaque sessionId on demande la dernière sous sessions correspondante et on obtient le sousSessionId.
pour chaque sousSessionId on demande le T-session-start et T-session-end de cette sousSessionId.
Si T-session-end > T-session-start > T-event-min alors cela veut dire que les évènements en cours 
ont probablement déjà été traités (dans un fonctionnement nominal). Que ce soit un rewind ou juste
un restart et que certaines sessions ont été enregistré sans que l'offset kafka ait été commité.

#### Effacer les toutes les sessions du futur

On efface toutes les sessions dont T-session-end >= T-event-min.

#### Récupérer tous les évènements des users pour la/les journées

On récupère tous les events pour les partyId données que l'on merge a nos events en input.

#### Reprocesser les sessions

On recréer les sessions à la main a partie de tous les events.
 
### Cas spécifique du restart

Pour moi le cas du restart ne pose presque pas de problème. Dans le pire des cas, le job précédents
a enregistrer des évènements dans ES et certaines sessions dans ES mais pas toutes (sinon l'offset kafka est avancé).

Dans ce cas la il faut s'assurer que les données de sessions ne soient pas corrompus (par exemple "eventCounter").

Le processe que l'on a mis en place pour le rewind devrait aussi fonctionner dans ce cas là. C'est à dire qu'il
devra détecter que els évènements on déjà été traité dans le cadre de certaines sessions, d'aller chercher les évènements
précédents et de recomputer la sessions puis de l'overrider. Il ne faut pas effacer la session en question car sinon
le job ne serait pas idempotent. Et que dans le cas dans double redémarrage rapide, le job ne serait plus capable
de détecter le restart car la sessions serait inexistante. Ou alors si l'on doit effacer des sessions, il ne faut le faire qu'a la fin après avoir injecté les nouvelles.
(si jamais il devait y'avoir au final moins de sessions qu'au début.)

### Validation avec des scénario de test


#### Rejoue du job 2 fois sur les même données

Faire au moins 2 tests

#### Rewind du job

Faire au moins 2 tests







