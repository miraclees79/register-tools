# -*- coding: utf-8 -*-
"""
Created on Wed May 13 13:49:53 2026

@author: U120137
"""

import requests
import time
import traceback

class KrsApiEnricher:
    # Zmiana z OdpisAktualny na OdpisPelny (daje dostęp do historii zmian)
    BASE_URL = "https://api-krs.ms.gov.pl/api/krs/OdpisPelny"

    def fetch_entity_data(self, krs_number: str) -> dict:
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

    def parse_krs_json(
        self, 
        json_data: dict
    ) -> dict:
        """Ekstrahuje informacje z Odpisu Pełnego API KRS wraz z datami historycznymi."""
        if "error" in json_data:
            return {
                "krs_status": json_data.get("error", ""),
                "likwidacja": "", 
                "krs_adres_aktualny": "", 
                "krs_adresy_historyczne": "",
                "osoby_decyzyjne": "", 
                "historyczne_osoby_decyzyjne": "",
                "udzialowcy": "",
                "historyczni_udzialowcy": ""
            }

        try:
            odpis = json_data.get("odpis", {})
            dane = odpis.get("dane", {})
            naglowekP = odpis.get("naglowekP", {})
            
            # 0. MAPOWANIE DAT WPISÓW
            # Tworzymy słownik, który pod numerem wpisu przechowuje datę, np. {"1": "18.11.2021"}
            wpisy = naglowekP.get("wpis", [])
            data_wpisow_map = {}
            for wpis_slownik in wpisy:
                nr = str(wpis_slownik.get("numerWpisu"))
                data = wpis_slownik.get("dataWpisu", "Brak daty")
                data_wpisow_map[nr] = data

            # Funkcja pomocnicza do pobierania daty
            def get_data_wpisu(nr_wpisu: str) -> str:
                if not nr_wpisu:
                    return ""
                # data_wpisow_map to standardowy słownik, więc wywołujemy get pozycyjnie
                return data_wpisow_map.get(str(nr_wpisu), f"wpis {nr_wpisu}")

            # 1. Status podmiotu
            stan = naglowekP.get("stanPozycji")
            status = "Aktywny" if stan == 1 else "Wykreślony/Inny"

            # 2. Informacja o likwidacji
            dzial6 = dane.get("dzial6", {})
            is_liquidated = "Tak" if dzial6 and dzial6.get("rozwiazanieUniewaznienie", {}).get("okreslenieOkolicznosci") else "Nie"

            # 3. Historia Adresów
            siedziba_i_adres = dane.get("dzial1", {}).get("siedzibaIAdres", {})
            adres_lista = siedziba_i_adres.get("adres", [])
            
            wszystkie_adresy = []
            for wpis_adresu in adres_lista:
                ulica = wpis_adresu.get("ulica", "")
                nr_domu = wpis_adresu.get("nrDomu", "")
                nr_lokalu = wpis_adresu.get("nrLokalu", "")
                miejscowosc = wpis_adresu.get("miejscowosc", "")
                kod_pocztowy = wpis_adresu.get("kodPocztowy", "")
                
                lokal_str = f"/{nr_lokalu}" if nr_lokalu else ""
                ulica_str = f"{ulica} {nr_domu}{lokal_str}".strip() if ulica else f"{nr_domu}{lokal_str}".strip()
                adres_str = f"{ulica_str}, {kod_pocztowy} {miejscowosc}".strip(" ,")
                
                if adres_str and (not wszystkie_adresy or wszystkie_adresy[-1] != adres_str):
                    wszystkie_adresy.append(adres_str)

            krs_adres_aktualny = wszystkie_adresy[-1] if wszystkie_adresy else ""
            krs_adresy_historyczne = " -> ".join(wszystkie_adresy[:-1]) if len(wszystkie_adresy) > 1 else "Brak zmian adresu"

            # 4. Osoby decyzyjne (Aktualne i historyczne)
            osoby_decyzyjne = []
            historyczne_osoby_decyzyjne = []
            
            reprezentacja_lista = dane.get("dzial2", {}).get("reprezentacja", [])
            sklad_zarzadu = reprezentacja_lista[-1].get("sklad", []) if reprezentacja_lista else []
            
            for osoba in sklad_zarzadu:
                nazwisko_historia = osoba.get("nazwisko", [])
                if not nazwisko_historia:
                    continue
                
                # Zawsze wyciągamy datę wejścia do zarządu i (ewentualnie) wyjścia z zarządu
                nr_wprow_zarzad = nazwisko_historia[0].get("nrWpisuWprow", "")
                nr_wykr_zarzad = nazwisko_historia[-1].get("nrWpisuWykr", "")
                
                data_wprow_zarzad = get_data_wpisu(nr_wpisu=nr_wprow_zarzad)
                
                imie_historia = osoba.get("imiona", [])
                imie = imie_historia[-1].get("imiona", {}).get("imie", "") if imie_historia else ""
                
                nazwisko_dict = nazwisko_historia[-1].get("nazwisko", {})
                nazwisko = nazwisko_dict.get("nazwiskoICzlon", nazwisko_dict.get("nazwiskoCzlonPierwszy", ""))
                
                identyfikator_historia = osoba.get("identyfikator", [])
                pesel = identyfikator_historia[-1].get("pesel", "") if identyfikator_historia else ""
                pesel_str = f" (PESEL: {pesel})" if pesel else ""
                
                funkcja_historia = osoba.get("funkcjaWOrganie", [])
                aktualna_funkcja = funkcja_historia[-1].get("funkcjaWOrganie", "Członek organu") if funkcja_historia else "Członek organu"
                
                # Dodano pesel_str do wynikowego stringa
                osoba_str = f"{imie} {nazwisko}{pesel_str} ({aktualna_funkcja})".strip()

                if nr_wykr_zarzad:
                    # Osoba została wykreślona
                    data_wykr_zarzad = get_data_wpisu(nr_wpisu=nr_wykr_zarzad)
                    historyczne_osoby_decyzyjne.append(f"{osoba_str} [od {data_wprow_zarzad} do {data_wykr_zarzad}]")
                else:
                    # Osoba jest aktywna w zarządzie
                    osoby_decyzyjne.append(f"{osoba_str} [od {data_wprow_zarzad}]")

            # 5. Udziałowcy (Aktualni i historyczni)
            udzialowcy = []
            historyczni_udzialowcy = []
            
            wspolnicy_lista = dane.get("dzial1", {}).get("wspolnicySpzoo", [])
            
            for wspolnik in wspolnicy_lista:
                nazwa_historia = wspolnik.get("nazwa", [])
                nazwisko_historia = wspolnik.get("nazwisko", [])
                
                udzialy_historia = wspolnik.get("posiadaneUdzialy", [])
                posiadane_udzialy = udzialy_historia[-1].get("posiadaneUdzialy", "") if udzialy_historia else ""

                if nazwa_historia:
                    # Udziałowiec to inna firma
                    nazwa_firmy = nazwa_historia[-1].get("nazwa", "")
                    nr_wprow_udzialy = nazwa_historia[0].get("nrWpisuWprow", "")
                    nr_wykr_udzialy = nazwa_historia[-1].get("nrWpisuWykr", "")
                    podmiot_str = f"{nazwa_firmy} [{posiadane_udzialy}]"
                elif nazwisko_historia:
                    # Udziałowiec to osoba fizyczna
                    imie_historia = wspolnik.get("imiona", [])
                    imie = imie_historia[-1].get("imiona", {}).get("imie", "") if imie_historia else ""
                    
                    nazwisko_dict = nazwisko_historia[-1].get("nazwisko", {})
                    nazwisko = nazwisko_dict.get("nazwiskoICzlon", "")
                    
                    # Pobieranie PESEL-u udziałowca
                    identyfikator_historia = wspolnik.get("identyfikator", [])
                    pesel = identyfikator_historia[-1].get("pesel", "") if identyfikator_historia else ""
                    pesel_str = f" (PESEL: {pesel})" if pesel else ""
                    
                    nr_wprow_udzialy = nazwisko_historia[0].get("nrWpisuWprow", "")
                    nr_wykr_udzialy = nazwisko_historia[-1].get("nrWpisuWykr", "")
                    
                    # Dodano pesel_str
                    podmiot_str = f"{imie} {nazwisko}{pesel_str} [{posiadane_udzialy}]".strip()
                else:
                    continue
                    
                data_wprow_udzialy = get_data_wpisu(nr_wpisu=nr_wprow_udzialy)

                if nr_wykr_udzialy:
                    # Udziałowiec sprzedał/pozbył się udziałów
                    data_wykr_udzialy = get_data_wpisu(nr_wpisu=nr_wykr_udzialy)
                    historyczni_udzialowcy.append(f"{podmiot_str} [od {data_wprow_udzialy} do {data_wykr_udzialy}]")
                else:
                    # Udziałowiec wciąż aktywny
                    udzialowcy.append(f"{podmiot_str} [od {data_wprow_udzialy}]")

            return {
                "krs_status": status,
                "likwidacja": is_liquidated,
                "krs_adres_aktualny": krs_adres_aktualny,
                "krs_adresy_historyczne": krs_adresy_historyczne,
                "osoby_decyzyjne": " | ".join(osoby_decyzyjne),
                "historyczne_osoby_decyzyjne": " | ".join(historyczne_osoby_decyzyjne),
                "udzialowcy": " | ".join(udzialowcy),
                "historyczni_udzialowcy": " | ".join(historyczni_udzialowcy)
            }

        except Exception:
            # traceback.format_exc() wyciągnie pełen opis błędu, w tym nr linii
            error_details = traceback.format_exc()
            print(f"BŁĄD PARSOWANIA JSON KRS:\n{error_details}")
            
            return {
                "krs_status": "Błąd struktury danych (sprawdź logi)",
                "likwidacja": "", 
                "krs_adres_aktualny": "", 
                "krs_adresy_historyczne": "",
                "osoby_decyzyjne": "",
                "historyczne_osoby_decyzyjne": "",
                "udzialowcy": "",
                "historyczni_udzialowcy": ""
            }