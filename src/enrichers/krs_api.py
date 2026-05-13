# -*- coding: utf-8 -*-
"""
Created on Wed May 13 13:49:53 2026

@author: U120137
"""

import requests
import time

class KrsApiEnricher:
    BASE_URL = "https://api-krs.ms.gov.pl/api/krs/OdpisAktualny"

    def fetch_entity_data(self, krs_number: str) -> dict:
        """Pobiera odpis aktualny z Rejestru Przedsiębiorców (rejestr=P)"""
        url = f"{self.BASE_URL}/{krs_number}?rejestr=P&format=json"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {"error": "Nie znaleziono w KRS (być może wykreślony lub inny rejestr)"}
            else:
                return {"error": f"Błąd API: {response.status_code}"}
        except requests.RequestException as e:
            return {"error": str(e)}

    def parse_krs_json(self, json_data: dict) -> dict:
        """Ekstrahuje podstawowe informacje (adres, status, osoby, udziałowcy) z JSONa KRS."""
        if "error" in json_data:
            return {"status": json_data["error"]}

        try:
            odpis = json_data.get("odpis", {})
            dane = odpis.get("dane", {})
            
            # 1. Status podmiotu
            stan = odpis.get("naglowekA", {}).get("stanPozycji", "Brak danych")
            status = "Aktywny" if stan == 1 else "Wykreślony/Inny"

            # Sprawdzenie czy w Dziale 6 (likwidacja) coś jest
            dzial6 = dane.get("dzial6", {})
            is_liquidated = "Tak" if dzial6.get("likwidacja") else "Nie"

            # 2. Adres
            adres_data = dane.get("dzial1", {}).get("siedzibaIAdres", {}).get("adres", {})
            adres = f"{adres_data.get('ulica', '')} {adres_data.get('nrDomu', '')}, {adres_data.get('kodPocztowy', '')} {adres_data.get('miejscowosc', '')}"

            # 3. Osoby decyzyjne (Reprezentacja - Dział 2)
            reprezentacja = dane.get("dzial2", {}).get("reprezentacja", {}).get("podmiot", {})
            osoby_decyzyjne =[]
            if isinstance(reprezentacja, dict) and "osobyWchodzaceWSkladOrganu" in reprezentacja:
                osoby = reprezentacja["osobyWchodzaceWSkladOrganu"]
                for osoba in osoby:
                    imiona = osoba.get("imiona", {}).get("imie", "")
                    nazwisko = osoba.get("nazwisko", {}).get("nazwiskoCzlonDrugi", osoba.get("nazwisko", {}).get("nazwiskoCzlonPierwszy", ""))
                    funkcja = osoba.get("funkcjaWOrganie", "")
                    osoby_decyzyjne.append(f"{imiona} {nazwisko} ({funkcja})")

            # 4. Udziałowcy (Dział 1 - Wspólnicy Sp. z o.o.)
            # Uwaga: struktura JSONa różni się dla S.A., PSA itp. Tu pokazuję Sp. z o.o. jako najczęstszą.
            wspolnicy_data = dane.get("dzial1", {}).get("wspolnicySpZoo", [])
            udzialowcy =[]
            for w in wspolnicy_data:
                imie = w.get("imiona", {}).get("imie", "")
                nazwisko = w.get("nazwisko", {}).get("nazwiskoCzlonPierwszy", w.get("nazwa", ""))
                udzialowcy.append(f"{imie} {nazwisko}".strip())

            return {
                "krs_status": status,
                "likwidacja": is_liquidated,
                "krs_adres": adres.strip(", "),
                "osoby_decyzyjne": "; ".join(osoby_decyzyjne),
                "udzialowcy": "; ".join(udzialowcy)
            }

        except Exception as e:
            return {"status": f"Błąd parsowania: {str(e)}"}