# Composant 1 – Simulateur InStockNow

Ce composant génère des messages simulés pour un système IoT de suivi de stock en magasin.
Il envoie toutes les 10 secondes un message Kafka pour **chaque produit** de la liste (10 au total),
avec un seuil (`threshold`) propre à chaque produit.

## Exécution

```bash
sbt run
```

Le producteur publie les messages dans le topic Kafka : `instocknow-input`

# Exemple de message

```
{
  "sensor_id": "shelf-2",
  "timestamp": "2025-07-05T13:52:00Z",
  "product_id": "GTA6",
  "quantity": 3,
  "threshold": 5,
  "location": {
    "store": "paris-12",
    "lat": 48.85,
    "lon": 2.34
  },
  "alert": true
}
```
