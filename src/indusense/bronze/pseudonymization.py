"""Pseudonymisation des données personnelles à l'ingestion : HMAC-SHA256 avec un sel secret.

Une colonne marquée `"pseudonymize": true` dans le contrat n'est jamais écrite en clair : le bronze reçoit
HMAC-SHA256(sel, valeur normalisée), en hexadécimal (64 caractères). C'est la seule transformation que le bronze
s'autorise (principe de minimisation, RGPD art. 5) : la valeur d'origine ne doit pas entrer en base.

- **Normalisation** (Unicode NFC, espaces retirés autour, casse ignorée) : « Lucas Bernard » et
  « lucas bernard » donnent le même pseudonyme, une même personne reste reconnaissable d'une ligne à l'autre.
- **Sel** : `ANONYMISATION_SALT` dans `.env`, jamais versionné. Sans le sel, on ne peut pas recalculer un
  pseudonyme à partir d'un nom ; avec lui, si. Le résultat est donc une **pseudonymisation**, pas une
  anonymisation : au sens du RGPD, la donnée reste personnelle et le sel doit être protégé comme un secret.
- Un changement de sel change tous les pseudonymes : les données déjà chargées ne se recoupent plus avec les
  nouvelles.
- Vide et NULL sont conservés tels quels : il n'y a rien à protéger, et hacher '' produirait un faux pseudonyme.
"""

import hashlib
import hmac
import unicodedata

PLACEHOLDER_SALTS = {"", "change-me"}


class MissingSalt(Exception):
    """Sel absent ou laissé à sa valeur d'exemple : aucune donnée personnelle n'est écrite."""


def check_salt(salt: str | None) -> str:
    """Retourne le sel, ou lève MissingSalt s'il est absent ou laissé à « change-me »."""
    if salt is None or salt.strip() in PLACEHOLDER_SALTS:
        raise MissingSalt("ANONYMISATION_SALT is missing or still set to its placeholder in .env: "
                          "personal data cannot be pseudonymized")
    return salt


def normalize(value: str) -> str:
    return unicodedata.normalize("NFC", value).strip().casefold()


def pseudonymize(value: str | None, salt: str) -> str | None:
    """HMAC-SHA256(sel, valeur normalisée) en hexadécimal ; vide et None sont retournés tels quels."""
    if value is None or not value.strip():
        return value
    return hmac.new(salt.encode("utf-8"), normalize(value).encode("utf-8"), hashlib.sha256).hexdigest()
