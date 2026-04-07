; ═══════════════════════════════════════════════════════════════════════════════
; Seam Analytics — Inno Setup Installer Script
; ═══════════════════════════════════════════════════════════════════════════════
;
; Build:  ISCC.exe installer\seam_setup.iss
;         (run from repo root, or adjust SourceDir below)

#define MyAppName      "Seam Analytics"
#define MyAppVersion   "1.0.0-beta"
#define MyAppPublisher "Seam Analytics"
#define MyAppExeName   "SeamAnalytics.exe"
#define MyUpdaterExe   "SeamUpdater.exe"

[Setup]
AppId={{B8A3D6F1-7C2E-4A91-9D0B-3E5F8C1A2B4D}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=output
OutputBaseFilename=SeamAnalytics-Setup-{#MyAppVersion}
Compression=lzma2/ultra64
SolidCompression=yes
SetupIconFile=..\app\assets\Logo.ico
UninstallDisplayIcon={app}\SeamAnalytics\{#MyAppExeName}
WizardStyle=modern
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
LicenseFile=
; Estimated installed size (MB): ~450 app + ~780 databases
ExtraDiskSpaceRequired=838860800

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon";    Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"
Name: "scheduledupdate"; Description: "Schedule daily data updates (recommended)"; GroupDescription: "Automatic Updates:"

[Files]
; ── Main application (onedir bundle) ──────────────────────────────────
Source: "dist\SeamAnalytics\*"; DestDir: "{app}\SeamAnalytics"; Flags: ignoreversion recursesubdirs createallsubdirs

; ── Updater (onedir bundle) ───────────────────────────────────────────
Source: "dist\SeamUpdater\*"; DestDir: "{app}\SeamUpdater"; Flags: ignoreversion recursesubdirs createallsubdirs

; ── Pre-loaded databases → user data dir ──────────────────────────────
Source: "..\app\mlb_raw.db";        DestDir: "{localappdata}\SeamAnalytics"; Flags: ignoreversion; Check: ShouldInstallDB('mlb_raw.db')
Source: "..\app\mlb_calculated.db"; DestDir: "{localappdata}\SeamAnalytics"; Flags: ignoreversion; Check: ShouldInstallDB('mlb_calculated.db')

[Dirs]
Name: "{localappdata}\SeamAnalytics"
Name: "{localappdata}\SeamAnalytics\assets\lineup_cache"
Name: "{localappdata}\SeamAnalytics\assets\weather_cache"
Name: "{localappdata}\SeamAnalytics\statcast_cache"

[Icons]
Name: "{group}\{#MyAppName}";           Filename: "{app}\SeamAnalytics\{#MyAppExeName}"; IconFilename: "{app}\SeamAnalytics\_internal\assets\Logo.ico"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}";     Filename: "{app}\SeamAnalytics\{#MyAppExeName}"; IconFilename: "{app}\SeamAnalytics\_internal\assets\Logo.ico"; Tasks: desktopicon

[Run]
; Launch app after install
Filename: "{app}\SeamAnalytics\{#MyAppExeName}"; Description: "Launch {#MyAppName}"; Flags: nowait postinstall skipifsilent

; Register scheduled task (only if user selected it)
Filename: "schtasks.exe"; \
  Parameters: "/Create /F /TN ""SeamAnalytics\DailyUpdate"" /TR """"""{app}\SeamUpdater\{#MyUpdaterExe}"""""" /SC DAILY /ST 06:00 /RL LIMITED"; \
  StatusMsg: "Creating scheduled update task..."; \
  Flags: runhidden; \
  Tasks: scheduledupdate

[UninstallRun]
; Remove scheduled task on uninstall
Filename: "schtasks.exe"; Parameters: "/Delete /F /TN ""SeamAnalytics\DailyUpdate"""; Flags: runhidden; RunOnceId: "RemoveTask"

[UninstallDelete]
; Clean up writable caches (not databases — user may want to keep those)
Type: filesandordirs; Name: "{localappdata}\SeamAnalytics\assets"
Type: filesandordirs; Name: "{localappdata}\SeamAnalytics\statcast_cache"
Type: files;          Name: "{localappdata}\SeamAnalytics\player_ids_cache.pkl"
Type: files;          Name: "{localappdata}\SeamAnalytics\temp_lineups_cache.json"
Type: files;          Name: "{localappdata}\SeamAnalytics\processed_dates.pkl"

[Code]
// Only install a DB if it doesn't already exist (don't overwrite user data on upgrade)
function ShouldInstallDB(DBName: String): Boolean;
begin
  Result := not FileExists(ExpandConstant('{localappdata}\SeamAnalytics\') + DBName);
end;
