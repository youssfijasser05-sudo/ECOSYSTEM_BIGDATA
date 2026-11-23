"""Simulateur d'événements Active Directory (remplace la VM dc01).
Envoie à Logstash des événements au format Winlogbeat (champs winlog.*).

Usage :
  python sim.py fond                   activité normale (connexion réussie d'alice toutes les 30 s)
  python sim.py succes <compte>        connexion réussie (4768 + 4624)
  python sim.py echec <compte> [N]     N mauvais mots de passe (4771, Status 0x18)
  python sim.py brute <compte>         6 échecs + verrouillage du compte (4771 x6 + 4740)
  python sim.py deverrouiller <compte> déverrouillage (4767)
"""
import datetime, json, os, socket, sys, time

HOST = os.environ.get("LOGSTASH_HOST", "logstash")
PORT = int(os.environ.get("LOGSTASH_PORT", "5046"))
IP_CLIENT = os.environ.get("IP_CLIENT", "192.168.100.1")

MESSAGES = {
    "4624": "L'ouverture de session d'un compte s'est correctement déroulée.",
    "4625": "Échec d'ouverture de session d'un compte.",
    "4768": "Un ticket d'authentification Kerberos (TGT) a été demandé.",
    "4771": "La pré-authentification Kerberos a échoué.",
    "4740": "Un compte d'utilisateur a été verrouillé.",
    "4767": "Un compte d'utilisateur a été déverrouillé.",
}

def evenement(event_id, compte, status=None):
    data = {"TargetUserName": compte, "IpAddress": "::ffff:" + IP_CLIENT}
    if status:
        data["Status"] = status
    maintenant = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return {
        "@timestamp": maintenant,
        "host": {"name": "dc01.isetlab.lan"},
        "winlog": {"channel": "Security", "event_id": event_id, "computer_name": "dc01.isetlab.lan", "event_data": data},
        "message": MESSAGES[event_id] + "\n(événement simulé - dc01-sim)",
    }

def envoyer(evenements):
    for essai in range(30):                      # Logstash peut mettre du temps à démarrer
        try:
            with socket.create_connection((HOST, PORT), timeout=10) as s:
                for e in evenements:
                    s.sendall((json.dumps(e, ensure_ascii=False) + "\n").encode("utf-8"))
                    time.sleep(0.05)
            for e in evenements:
                print(f"[dc01-sim] envoyé {e['winlog']['event_id']} pour {e['winlog']['event_data']['TargetUserName']}", flush=True)
            return
        except OSError as err:
            print(f"[dc01-sim] Logstash injoignable ({err}), nouvel essai dans 5 s", flush=True)
            time.sleep(5)

def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "fond"
    compte = sys.argv[2] if len(sys.argv) > 2 else "bob.test"
    if cmd == "fond":
        while True:
            envoyer([evenement("4768", "alice.test", "0x0"), evenement("4624", "alice.test")])
            time.sleep(30)
    elif cmd == "succes":
        envoyer([evenement("4768", compte, "0x0"), evenement("4624", compte)])
    elif cmd == "echec":
        n = int(sys.argv[3]) if len(sys.argv) > 3 else 1
        envoyer([evenement("4771", compte, "0x18") for _ in range(n)])
    elif cmd == "brute":
        envoyer([evenement("4771", compte, "0x18") for _ in range(5)] + [evenement("4740", compte), evenement("4771", compte, "0x18")])
    elif cmd == "deverrouiller":
        envoyer([evenement("4767", compte)])
    else:
        print(__doc__)

if __name__ == "__main__":
    main()
