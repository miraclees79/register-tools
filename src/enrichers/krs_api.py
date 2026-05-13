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
        """Ekstrahuje podstawowe informacje z JSONa z API KRS."""
        if "error" in json_data:
            return {
                "krs_status": json_data["error"],
                "likwidacja": "", "krs_adres": "", "osoby_decyzyjne": "", "udzialowcy": ""
            }

        try:
            odpis = json_data.get("odpis", {})
            dane = odpis.get("dane", {})
            
            # 1. Status podmiotu
            # stanPozycji = 1 oznacza podmiot wpisany do rejestru (aktywny)
            stan = odpis.get("naglowekA", {}).get("stanPozycji")
            status = "Aktywny" if stan == 1 else "Wykreślony/Inny"

            # 2. Informacja o likwidacji
            # Dział 6 dla aktywnych spółek bez likwidacji to zazwyczaj puste {}
            dzial6 = dane.get("dzial6", {})
            is_liquidated = "Tak" if dzial6 else "Nie"

            # 3. Formatowanie adresu
            adres_data = dane.get("dzial1", {}).get("siedzibaIAdres", {}).get("adres", {})
            ulica = adres_data.get("ulica", "")
            nr_domu = adres_data.get("nrDomu", "")
            nr_lokalu = adres_data.get("nrLokalu", "")
            miejscowosc = adres_data.get("miejscowosc", "")
            kod_pocztowy = adres_data.get("kodPocztowy", "")
            
            # Konstrukcja np. "Krucza 16/22"
            lokal_str = f"/{nr_lokalu}" if nr_lokalu else ""
            ulica_str = f"{ulica} {nr_domu}{lokal_str}".strip() if ulica else f"{nr_domu}{lokal_str}".strip()
            
            adres = f"{ulica_str}, {kod_pocztowy} {miejscowosc}".strip(" ,")

            # 4. Osoby decyzyjne (Dział 2 - Zarząd / Reprezentacja)
            osoby_decyzyjne =[]
            sklad_zarzadu = dane.get("dzial2", {}).get("reprezentacja", {}).get("sklad",[])
            for osoba in sklad_zarzadu:
                imie = osoba.get("imiona", {}).get("imie", "")
                nazwisko_dict = osoba.get("nazwisko", {})
                # Obsługa nazwisk jednoczłonowych i wieloczłonowych
                nazwisko = nazwisko_dict.get("nazwiskoICzlon", nazwisko_dict.get("nazwiskoCzlonPierwszy", ""))
                funkcja = osoba.get("funkcjaWOrganie", "Członek organu")
                
                osoby_decyzyjne.append(f"{imie} {nazwisko} ({funkcja})".strip())

            # 5. Udziałowcy (Wspólnicy Sp. z o.o.)
            # Uwaga: Dla spółek akcyjnych (S.A.) ten klucz to np. `jedynyAkcjonariusz` lub brak danych.
            udzialowcy =[]
            wspolnicy_data = dane.get("dzial1", {}).get("wspolnicySpzoo",[])
            for w in wspolnicy_data:
                posiadane_udzialy = w.get("posiadaneUdzialy", "")
                nazwa_firmy = w.get("nazwa", "")
                
                if nazwa_firmy:
                    # Udziałowcem jest inny podmiot (Spółka z o.o., LTD itp.)
                    udzialowcy.append(f"{nazwa_firmy} [{posiadane_udzialy}]")
                else:
                    # Udziałowcem jest osoba fizyczna
                    imie = w.get("imiona", {}).get("imie", "")
                    nazwisko_dict = w.get("nazwisko", {})
                    nazwisko = nazwisko_dict.get("nazwiskoICzlon", "")
                    
                    udzialowcy.append(f"{imie} {nazwisko} [{posiadane_udzialy}]".strip())

            return {
                "krs_status": status,
                "likwidacja": is_liquidated,
                "krs_adres": adres,
                "osoby_decyzyjne": " | ".join(osoby_decyzyjne),
                "udzialowcy": " | ".join(udzialowcy)
            }

        except Exception as e:
            return {
                "krs_status": f"Błąd parsowania: {str(e)}",
                "likwidacja": "", "krs_adres": "", "osoby_decyzyjne": "", "udzialowcy": ""
            }