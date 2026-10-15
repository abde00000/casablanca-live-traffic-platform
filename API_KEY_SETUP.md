# Put your TomTom API key in the right place

Your key goes in a private file named **`.env`** inside the project folder. Do not put it in this chat, in a Python file, in `README.md`, or in `.env.example`.

## Step 1 — Open the project folder

Open this folder in your file manager or code editor:

`casablanca-live-traffic-platform`

You should see a file named `.env.example` in it.

## Step 2 — Make a copy named `.env`

Do not rename or overwrite `.env.example`; make a copy of it.

**Windows PowerShell:** open PowerShell in the project folder and run:

```powershell
Copy-Item .env.example .env
notepad .env
```

**macOS or Linux:** open Terminal in the project folder and run:

```bash
cp .env.example .env
```

Then open the new `.env` file in a text editor.

## Step 3 — Replace the placeholder line

Find this line in `.env`:

```text
TOMTOM_API_KEY=replace_with_your_private_key
```

Replace only the placeholder after the equals sign with the key from your TomTom developer account. It should look like this, but with your actual key:

```text
TOMTOM_API_KEY=paste_your_actual_key_here
```

Do not type the words `paste_your_actual_key_here`; replace them with your key. Keep the line on one line, with no spaces around `=` and normally no quotation marks. Save the file.

## Step 4 — Check the filename

The file must be named **`.env`**, not `.env.example` and not `.env.txt`.

In PowerShell, check with:

```powershell
Get-ChildItem -Force .env*
```

On macOS/Linux, check with:

```bash
ls -a .env*
```

You should see both `.env` and `.env.example`.

## Step 5 — Start collection after the streams are ready

From the project folder, start the infrastructure and follow the separate-terminal steps in `commands.txt` to start Bronze, Silver, and Gold. After those jobs are running, start the live collector:

```bash
docker compose --profile live up -d --build traffic-collector
```

To watch its status:

```bash
docker compose logs -f traffic-collector
```

If the collector reports that the key is missing or invalid, stop it, correct the line in `.env`, save, and start the collector again. Never send the key or a screenshot that shows it to anyone.

The project ignores `.env` so the key is not added to Git by default. The key is passed only to the collector container; it is not written into the traffic event records.
