# Poprawki konsoli — 22.10.2026

Formularz i historia są w jednej kolumnie. Usunięto boczną nawigację, slogany,
sztuczne etykiety i powtarzające się panele architektury. Rzeczywiste stany,
workery, wyniki, próby, limity i błędy pozostają widoczne.
Dodano obsługę klawiatury zakładek, pułapkę fokusu w szczegółach, Escape,
blokadę tła i powrót do zadania. Zapytania mają limit 15 s i czytelny błąd
nieprawidłowej odpowiedzi. Zwiększono drobny tekst i poprawiono kontrast.

Build TypeScript/Vite przeszedł. Backend Rust i prywatna instancja nie zostały
zmienione. Zaktualizowano wyłącznie publiczną usługę console na ARM64;
workery i PostgreSQL zachowano. Wydanie `audit-20261022`,
`/home/pi/apps/taskmesh-public/current`, konfiguracja `compose.public.yaml`.
Sekrety są wyłącznie w prywatnym `.env`. Poprzedni obraz zachowano.

Zewnętrzne scenariusze i dowody przeglądarkowe:
https://github.com/Adam-Sapiecha/Portfolio-AdamSapiecha/actions/workflows/public-demos.yml.
Każdy przebieg używa nowych sesji, rzeczywistego backendu, walidacji TLS i szerokości
1280/390/360 px. Nie jest to pełna certyfikacja WCAG ani test każdej sieci firmowej.
