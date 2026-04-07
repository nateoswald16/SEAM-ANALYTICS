; ═══════════════════════════════════════════════════════════════════════════════
; Seam Analytics — Inno Setup Installer Script
; ═══════════════════════════════════════════════════════════════════════════════
;
; Build:  ISCC.exe installer\seam_setup.iss
;         (run from repo root, or adjust SourceDir below)

#define MyAppName      "Seam Analytics"
#define MyAppVersion   "1.0.1-beta"
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
PrivilegesRequiredOverridesAllowed=dialog commandline
ArchitecturesAllowed=x64compatible
CloseApplications=force
CloseApplicationsFilter=SeamAnalytics.exe,SeamUpdater.exe
LicenseFile=
; Estimated installed size (MB): ~450 app + ~780 databases
ExtraDiskSpaceRequired=838860800

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon";    Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"
Name: "scheduledupdate"; Description: "Schedule daily data updates (recommended)"; GroupDescription: "Automatic Updates:"; Flags: checkedonce

[Files]
; ── Main application (onedir bundle) ──────────────────────────────────
Source: "dist\SeamAnalytics\*"; DestDir: "{app}\SeamAnalytics"; Flags: ignoreversion recursesubdirs createallsubdirs

; ── Updater (onedir bundle) ───────────────────────────────────────────
Source: "dist\SeamUpdater\*"; DestDir: "{app}\SeamUpdater"; Flags: ignoreversion recursesubdirs createallsubdirs

; ── Pre-loaded databases → user data dir ──────────────────────────────
Source: "..\app\mlb_raw.db";        DestDir: "{localappdata}\SeamAnalytics"; Flags: ignoreversion uninsneveruninstall; Check: ShouldInstallDB('mlb_raw.db')
Source: "..\app\mlb_calculated.db"; DestDir: "{localappdata}\SeamAnalytics"; Flags: ignoreversion uninsneveruninstall; Check: ShouldInstallDB('mlb_calculated.db')

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
// Only install a DB if it doesn't already exist with real data (don't overwrite user data on upgrade)
function ShouldInstallDB(DBName: String): Boolean;
var
  Path: String;
  FindRec: TFindRec;
begin
  Path := ExpandConstant('{localappdata}\SeamAnalytics\') + DBName;
  if FindFirst(Path, FindRec) then
  begin
    // File exists — install only if it's a 0-byte stub from a failed run
    Result := (FindRec.SizeHigh = 0) and (FindRec.SizeLow = 0);
    FindClose(FindRec);
  end
  else
    // File doesn't exist — install it
    Result := True;
end;

// ── Custom time-picker page for scheduled updates ──
var
  TimePage: TWizardPage;
  HourCombo: TNewComboBox;
  MinuteCombo: TNewComboBox;
  AmPmCombo: TNewComboBox;

procedure InitializeWizard;
var
  Lbl: TNewStaticText;
  i: Integer;
begin
  TimePage := CreateCustomPage(wpSelectTasks,
    'Daily Update Time',
    'Choose what time the automatic data update should run each day.');

  Lbl := TNewStaticText.Create(TimePage);
  Lbl.Parent := TimePage.Surface;
  Lbl.Caption := 'Update time:';
  Lbl.Top := 24;
  Lbl.Left := 0;

  HourCombo := TNewComboBox.Create(TimePage);
  HourCombo.Parent := TimePage.Surface;
  HourCombo.Style := csDropDownList;
  HourCombo.Top := 20;
  HourCombo.Left := 90;
  HourCombo.Width := 55;
  for i := 1 to 12 do
    HourCombo.Items.Add(IntToStr(i));
  HourCombo.ItemIndex := 5;  // default = 6

  Lbl := TNewStaticText.Create(TimePage);
  Lbl.Parent := TimePage.Surface;
  Lbl.Caption := ':';
  Lbl.Top := 24;
  Lbl.Left := 150;

  MinuteCombo := TNewComboBox.Create(TimePage);
  MinuteCombo.Parent := TimePage.Surface;
  MinuteCombo.Style := csDropDownList;
  MinuteCombo.Top := 20;
  MinuteCombo.Left := 160;
  MinuteCombo.Width := 55;
  MinuteCombo.Items.Add('00');
  MinuteCombo.Items.Add('15');
  MinuteCombo.Items.Add('30');
  MinuteCombo.Items.Add('45');
  MinuteCombo.ItemIndex := 0;  // default = :00

  AmPmCombo := TNewComboBox.Create(TimePage);
  AmPmCombo.Parent := TimePage.Surface;
  AmPmCombo.Style := csDropDownList;
  AmPmCombo.Top := 20;
  AmPmCombo.Left := 225;
  AmPmCombo.Width := 55;
  AmPmCombo.Items.Add('AM');
  AmPmCombo.Items.Add('PM');
  AmPmCombo.ItemIndex := 0;  // default = AM

  Lbl := TNewStaticText.Create(TimePage);
  Lbl.Parent := TimePage.Surface;
  Lbl.Caption := 'Tip: Choose a time when your PC is usually on but you are not using the app.';
  Lbl.Top := 56;
  Lbl.Left := 0;
end;

function ShouldSkipPage(PageID: Integer): Boolean;
begin
  Result := False;
  if (PageID = TimePage.ID) and (not WizardIsTaskSelected('scheduledupdate')) then
    Result := True;
end;

function GetScheduleTime(Param: String): String;
var
  Hour24, Hour12, MinIdx: Integer;
begin
  Hour12 := StrToInt(HourCombo.Items[HourCombo.ItemIndex]);
  MinIdx := MinuteCombo.ItemIndex;

  // Convert 12-hour to 24-hour
  if AmPmCombo.ItemIndex = 0 then  // AM
  begin
    if Hour12 = 12 then
      Hour24 := 0
    else
      Hour24 := Hour12;
  end
  else  // PM
  begin
    if Hour12 = 12 then
      Hour24 := 12
    else
      Hour24 := Hour12 + 12;
  end;

  Result := Format('%.2d:%s', [Hour24, MinuteCombo.Items[MinIdx]]);
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ResultCode: Integer;
  TimeStr: String;
begin
  if CurStep = ssPostInstall then
  begin
    if WizardIsTaskSelected('scheduledupdate') then
    begin
      TimeStr := GetScheduleTime('');
      Exec('schtasks.exe',
        '/Create /F /TN "SeamAnalytics\DailyUpdate"' +
        ' /TR "' + ExpandConstant('{app}') + '\SeamUpdater\' + ExpandConstant('{#MyUpdaterExe}') + '"' +
        ' /SC DAILY /ST ' + TimeStr + ' /RL LIMITED',
        '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
    end;
  end;
end;
