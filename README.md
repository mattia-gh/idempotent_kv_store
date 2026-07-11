# idempotent_kv_store
Progetto 3: Architetture dei Sistemi Distribuiti.

Componenti: Dimitri Mattozzi, Mattia Bove.

# KV Store con retry idempotenti

Obiettivo: rendere sicuri i retry delle operazioni mutative.

## Comandi

- `PING`
- `GET <key>`
- `GETV <key>`
- `SET_REQ <client_id:sequence> <key> <value...>`
- `CAS_REQ <client_id:sequence> <key> <expected_version> <value...>`
- `DELETE_REQ <client_id:sequence> <key>`
- `QUIT`

## Semantica

Il server ricorda l'esito delle richieste mutative gia' viste.

Se il client ritenta con lo stesso `request_id`, il server:
- non riapplica l'effetto;
- restituisce la stessa risposta gia' prodotta.

`GET` restituisce solo il valore, come nel KV store base.
`GETV` restituisce valore e versione, come nel laboratorio sul CAS.

Precondizioni principali:

- `request_id` deve avere forma `<client_id>:<sequence_number>`;
- `sequence_number` deve essere un intero non negativo;
- lo stesso `request_id` deve essere usato solo per lo stesso comando logico;
- il client deve usare numeri di sequenza monotoni per ogni `client_id`.

Postcondizioni principali:

- `SET_REQ` crea o aggiorna la chiave e assegna la versione successiva;
- `CAS_REQ` modifica la chiave solo se la versione corrente coincide con quella attesa;
- `DELETE_REQ` rimuove la chiave se esiste;
- il retry di una richiesta gia' vista non modifica di nuovo lo stato.

## Contratto del request id

`request_id = <client_id>:<sequence_number>`

Il client deve usare `sequence_number` monotoni per client.

Lo stesso `request_id` identifica la stessa richiesta mutativa.
Se un client riusa lo stesso `request_id` con comando, chiave, versione attesa
o valore diversi, il server risponde:

```text
ERR request_id_reused
```

e non modifica lo stato dello store.

Il server conserva solo gli ultimi `N` request id per client:
- `N = 100`

Quindi un request id piu' vecchio puo' essere dimenticato.
Se il client ritenta dopo quella finestra, il retry non e' piu' garantito come idempotente.

## Safety

- la stessa richiesta mutativa non produce effetti doppi;
- due richieste diverse non vengono confuse solo perche' toccano la stessa chiave;
- il replay della risposta e' coerente con l'effetto gia' applicato.

## Liveness

- il server non conserva per sempre tutti i request id;
- la garbage collection locale non blocca il servizio;
- un client corretto puo' completare una sequenza di retry finche' resta nella finestra conservata.

## Nota tecnica

La soluzione resta single-node e volatile, come i primi laboratori del KV store.
La tabella dei request id e' protetta dallo stesso lock dello store: questo
rende atomico il controllo "gia' visto?" + effetto + memorizzazione della
risposta, ma serializza le operazioni concorrenti.

Trade-off scelto:

- memoria limitata a `N` richieste per client invece di conservazione infinita;
- contratto semplice, basato su numeri di sequenza monotoni;
- nessuna persistenza della tabella, quindi la garanzia non sopravvive al riavvio.

Possibili evoluzioni:

- rendere persistente la tabella dei request id;
- usare ack cumulativi del client per pulire in modo piu' preciso;
- usare lock per chiave per aumentare il parallelismo.

## Esecuzione su Ubuntu

Avvio server:

```bash
python3 server.py
```

Client interattivo:

```bash
python3 client.py
```

Test di accettazione:

```bash
python3 acceptance_test.py
```


## Comandi aggiuntivi

- `STATUS` mostra numero di chiavi, client e request id memorizzati.

Il server registra inoltre log espliciti per replay idempotenti e garbage collection.
