console.log("[cmp.js] Loaded");

let selectedAccount = null;
let uploadedFile = null;

/**
 * Load accounts from API and populate the dropdown
 */
async function loadAccounts() {
    const accountSelect = document.getElementById("account-select");
    if (!accountSelect) {
        console.warn("[loadAccounts] account-select not found");
        return;
    }

    try {
        console.log("[loadAccounts] fetching /api/accounts");
        const resp = await fetch("/api/accounts", {
            method: "GET"
        });

        console.log("[loadAccounts] /api/accounts status:", resp.status);
        if (!resp.ok) {
            const text = await resp.text();
            console.error("[loadAccounts] failed:", text);
            accountSelect.innerHTML = '<option value="">Failed to load accounts</option>';
            return;
        }

        const data = await resp.json();
        console.log("[loadAccounts] data:", data);

        const accounts = data.accounts || [];

        // Clear and populate dropdown
        accountSelect.innerHTML = '<option value="">-- Select an account --</option>';
        accounts.forEach(email => {
            const option = document.createElement("option");
            option.value = email;
            option.textContent = email;
            accountSelect.appendChild(option);
        });

        console.log("[loadAccounts] populated with", accounts.length, "accounts");
    } catch (err) {
        console.error("[loadAccounts] error:", err);
        accountSelect.innerHTML = '<option value="">Error loading accounts</option>';
    }
}

/**
 * Handle account selection change
 */
function handleAccountChange(event) {
    selectedAccount = event.target.value;
    console.log("[handleAccountChange] selected:", selectedAccount);

    if (selectedAccount) {
        console.log("[handleAccountChange] Account selected, showing Step 2");
        showStep2();
    }
}

/**
 * Show Step 2 with fade-in animation
 */
function showStep2() {
    const step2 = document.getElementById("step-2");
    if (!step2) return;

    step2.style.display = "block";
    step2.classList.add("fade-in");

    console.log("[showStep2] Step 2 displayed");
}

function handleFileSelect(event) {
    const file = event.target.files[0];
    if (!file) return;

    console.log("[handleFileSelect] file selected:", file.name);
    uploadedFile = file;

    const fileInfoRow = document.getElementById("file-info-row");
    const fileName = document.getElementById("file-name");
    const uploadLabel = document.querySelector(".upload-label");
    const generateBtn = document.getElementById("generate-preview-btn");

    if (fileInfoRow && fileName && uploadLabel && generateBtn) {
        fileName.textContent = file.name;
        uploadLabel.style.display = "none";
        fileInfoRow.style.display = "flex";
        generateBtn.style.display = "block";
    }
}

function removeFile() {
    console.log("[removeFile] removing file");
    uploadedFile = null;

    const fileInput = document.getElementById("excel-file-input");
    const fileInfoRow = document.getElementById("file-info-row");
    const uploadLabel = document.querySelector(".upload-label");
    const generateBtn = document.getElementById("generate-preview-btn");

    if (fileInput) fileInput.value = "";
    if (fileInfoRow) fileInfoRow.style.display = "none";
    if (uploadLabel) uploadLabel.style.display = "flex";
    if (generateBtn) generateBtn.style.display = "none";
}

async function downloadTemplate() {
    console.log("[downloadTemplate] initiating download");
    try {
        const resp = await fetch("/api/template");
        if (!resp.ok) {
            console.error("[downloadTemplate] failed:", resp.status);
            alert("Failed to download template");
            return;
        }

        const blob = await resp.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "campaign_sheet_template.xlsx";
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);

        console.log("[downloadTemplate] success");
    } catch (err) {
        console.error("[downloadTemplate] error:", err);
        alert("Error downloading template");
    }
}

/**
 * Generate preview from uploaded file
 */
async function generatePreview() {
    if (!uploadedFile) {
        alert("Please upload a file first");
        return;
    }

    if (!selectedAccount) {
        alert("Please select an account first");
        return;
    }

    console.log("[generatePreview] generating preview for:", uploadedFile.name);

    alert("Preview generation will be implemented in the next step");
}

// Initialize on page load
document.addEventListener("DOMContentLoaded", () => {
    console.log("[cmp.js] DOMContentLoaded");

    loadAccounts();

    const accountSelect = document.getElementById("account-select");
    if (accountSelect) {
        accountSelect.addEventListener("change", handleAccountChange);
    }

    const downloadBtn = document.getElementById("download-template-btn");
    if (downloadBtn) {
        downloadBtn.addEventListener("click", downloadTemplate);
    }

    const fileInput = document.getElementById("excel-file-input");
    if (fileInput) {
        fileInput.addEventListener("change", handleFileSelect);
    }

    const removeBtn = document.getElementById("remove-file-btn");
    if (removeBtn) {
        removeBtn.addEventListener("click", removeFile);
    }

    const generateBtn = document.getElementById("generate-preview-btn");
    if (generateBtn) {
        generateBtn.addEventListener("click", generatePreview);
    }
});

window.loadAccounts = loadAccounts;
window.downloadTemplate = downloadTemplate;
window.generatePreview = generatePreview;
