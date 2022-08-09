# skyportal-dumps

#### SkyPortal Dumps is a tool that allows a user to generate dumps of the DB as .YAML files that can be loaded in SkyPortal using `make load_demo_data`.

As of right now, the code creates dumps for the sources and photometry. As those 2 are linked to instruments, telescopes and groups, you need to post those first to the database.
Later on, we plan on adding the dumps of the instruments, telescopes and groups.
