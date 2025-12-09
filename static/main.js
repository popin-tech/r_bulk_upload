const uploadSection = document.getElementById("upload-section");
const previewSection = document.getElementById("preview-section");
const userInfo = document.getElementById("user-info");
const previewTable = document.getElementById("preview-table");
const previewMeta = document.getElementById("preview-meta");
const uploadForm = document.getElementById("upload-form");
const commitButton = document.getElementById("commit-button");
const templateSection = document.getElementById("template-section");
const downloadTemplateButton = document.getElementById("download-template-button");
const accountSection = document.getElementById("account-section");
const accountSelect = document.getElementById("account-select");

let idToken = "";
let lastPreview = null;

console.log("[main.js] Loaded");
console.log("[main.js] DOM refs:", {
  uploadSection,
  previewSection,
  userInfo,
  previewTable,
  previewMeta,
  uploadForm,
  commitButton,
  templateSection,
  downloadTemplateButton,
  accountSection,
  accountSelect,
});

function replaceGoogleButton(email) {
  console.log("[replaceGoogleButton] for:", email);
  const container = document.getElementById("g_id_signin_container");
  if (!container) {
    console.error("[replaceGoogleButton] #g_id_signin_container not found");
    return;
  }
  container.innerHTML = "";
  const logged = document.createElement("div");
  logged.className = "logged-in-button";
  logged.textContent = `Logged in as ${email}, click to log out.`;
  logged.addEventListener("click", () => {
    console.log("[replaceGoogleButton] logout clicked");
    logout();
  });
  container.appendChild(logged);
}

function logout() {
  console.log("[logout] clearing state & reloading");
  idToken = "";
  lastPreview = null;
  toggleSection(templateSection, false);
  toggleSection(uploadSection, false);
  toggleSection(previewSection, false);
  if (accountSection) toggleSection(accountSection, false);
  location.reload();
}

async function loadAccounts() {
  if (!accountSelect) {
    console.warn("[loadAccounts] accountSelect not found");
    return;
  }
  if (!idToken) {
    console.warn("[loadAccounts] no idToken, abort");
    return;
  }

  try {
    console.log("[loadAccounts] fetching /api/accounts");
    const resp = await fetch("/api/accounts", {
      method: "GET",
      headers: {
        Authorization: `Bearer ${idToken}`,
      },
    });

    console.log("[loadAccounts] /api/accounts status:", resp.status);
    if (!resp.ok) {
      console.error("[loadAccounts] failed:", await resp.text());
      return;
    }

    const data = await resp.json();
    const accounts = data.accounts || [];

    // Reset options
    accountSelect.innerHTML = "";
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.disabled = true;
    placeholder.selected = true;
    placeholder.textContent = "Select an account";
    accountSelect.appendChild(placeholder);

    accounts.forEach((email) => {
      const opt = document.createElement("option");
      opt.value = email;
      opt.textContent = email;
      accountSelect.appendChild(opt);
    });

    console.log("[loadAccounts] loaded accounts:", accounts.length);
  } catch (err) {
    console.error("[loadAccounts] error:", err);
  }
}

function toggleSection(section, visible) {
  if (!section) {
    console.warn("[toggleSection] null section, visible =", visible);
    return;
  }
  console.log(`[toggleSection] ${section.id} =>`, visible ? "show" : "hide");
  section.classList.toggle("hidden", !visible);
}

async function handleGoogleCredential(response) {
  console.log("[handleGoogleCredential] GIS callback:", response);
  idToken = response.credential;

  try {
    console.log("[handleGoogleCredential] calling /api/me");
    const meResp = await fetch("/api/me", {
      method: "GET",
      headers: {
        Authorization: `Bearer ${idToken}`,
      },
    });

    console.log("[handleGoogleCredential] /api/me status:", meResp.status);
    if (!meResp.ok) {
      const text = await meResp.text();
      console.error("[handleGoogleCredential] Auth failed:", text);
      userInfo.textContent = "You are not authorized to use this app.";
      toggleSection(templateSection, false);
      toggleSection(uploadSection, false);
      toggleSection(previewSection, false);
      if (accountSection) toggleSection(accountSection, false);
      idToken = "";
      return;
    }
    const data = await meResp.json();
    console.log("[handleGoogleCredential] /api/me data:", data);
    const email = data.user?.email || "";
    if (userInfo) {
      userInfo.textContent = "";
    }
    replaceGoogleButton(email);
    toggleSection(templateSection, true);
    toggleSection(uploadSection, true);
    if (accountSection) toggleSection(accountSection, true);
    loadAccounts();
  } catch (err) {
    console.error("[handleGoogleCredential] Error calling /api/me:", err);
    userInfo.textContent = "Authentication failed. Please try again.";
    toggleSection(templateSection, false);
    toggleSection(uploadSection, false);
    toggleSection(previewSection, false);
    if (accountSection) toggleSection(accountSection, false);
    idToken = "";
  }
}

window.handleGoogleCredential = handleGoogleCredential;
console.log("[main.js] window.handleGoogleCredential set:", !!window.handleGoogleCredential);

if (downloadTemplateButton) {
  downloadTemplateButton.addEventListener("click", async () => {
    console.log("[downloadTemplateButton] click");
    if (!idToken) {
      alert("Please sign in first.");
      return;
    }

    try {
      const response = await fetch("/api/template", {
        method: "GET",
        headers: {
          Authorization: `Bearer ${idToken}`,
        },
      });

      console.log("[downloadTemplateButton] /api/template status:", response.status);
      if (!response.ok) {
        throw new Error(await response.text());
      }

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "campaign_sheet_template.xlsx";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error("Download failed:", err);
      alert("Download failed. Please try again.");
    }
  });
} else {
  console.error("[main.js] #download-template-button not found");
}

function renderPreview(preview) {
  console.log("[renderPreview] preview:", preview);
  const { columns, rows, total_rows: totalRows, preview_count: previewCount } = preview;
  previewMeta.textContent = `Showing ${previewCount} rows (of ${totalRows}).`;

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((col) => {
      const td = document.createElement("td");
      td.textContent = row[col] ?? "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  previewTable.replaceChildren(table);
  toggleSection(previewSection, true);
}

if (uploadForm) {
  uploadForm.addEventListener("submit", async (event) => {
    event.preventDefault();

    if (!idToken) {
      alert("Please sign in first.");
      return;
    }

    const fileInput = uploadForm.querySelector('input[type="file"]');
    const file = fileInput?.files?.[0];

    if (!file) {
      alert("Please choose an Excel file before generating preview.");
      return;
    }

    const submitButton = uploadForm.querySelector('button[type="submit"], button');
    if (submitButton) {
      submitButton.disabled = true;
      submitButton.textContent = "Generating Preview...";
    }

    try {
      const formData = new FormData(uploadForm);

      const response = await fetch("/api/upload-preview", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${idToken}`,
        },
        body: formData,
      });

      console.log("[uploadForm] /api/upload-preview status:", response.status);

      if (!response.ok) {
        const errText = await response.text();
        throw new Error(errText || "Upload preview failed");
      }

      const { preview } = await response.json();
      console.log("[uploadForm] preview data:", preview);

      lastPreview = preview;
      renderPreview(preview);

      // allow commit only after preview is ready (optional)
      if (commitButton) {
        commitButton.disabled = false;
      }
    } catch (error) {
      console.error("[uploadForm] /api/upload-preview failed:", error);
      alert(`Generate preview failed: ${error.message || error}`);
    } finally {
      if (submitButton) {
        submitButton.disabled = false;
        submitButton.textContent = "Generate Preview";
      }
    }
  });
} else {
  console.error("[main.js] #upload-form not found");
}

if (commitButton) {
  commitButton.addEventListener("click", async () => {
    console.log("[commitButton] click");
    if (!lastPreview) {
      alert("No preview data to commit.");
      return;
    }

    const fileInput = uploadForm?.querySelector('input[type="file"]');
    const file = fileInput?.files?.[0];
    if (!file) {
      alert("Please choose an Excel file again before committing.");
      return;
    }

    let accountEmail = "";
    if (accountSelect) {
      accountEmail = accountSelect.value;
      console.log("[commitButton] accountEmail:", accountEmail);
      if (!accountEmail) {
        alert("Please select an account before syncing.");
        return;
      }
    } else {
      console.warn("[commitButton] accountSelect not found");
    }

    commitButton.disabled = true;
    commitButton.textContent = "Syncing...";
    try {
      const formData = new FormData();
      formData.append("file", file);
      if (accountEmail) {
        formData.append("account_email", accountEmail);
      }

      const response = await fetch("/api/commit", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${idToken}`,
        },
        body: formData,
      });
      console.log("[commitButton] /api/commit status:", response.status);
      if (!response.ok) {
        throw new Error(await response.text());
      }
      alert("Changes sent to Broadciel.");
    } catch (error) {
      console.error("[commitButton] Commit failed:", error);
      alert(`Commit failed: ${error}`);
    } finally {
      commitButton.disabled = false;
      commitButton.textContent = "Confirm & Sync";
    }
  });
} else {
  console.error("[main.js] #commit-button not found");
}