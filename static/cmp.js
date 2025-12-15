console.log("[cmp.js] Loaded");

let selectedAccount = null;
let uploadedFile = null;

/**
 * Load accounts from API and populate the dropdown
 */
async function loadAccounts() {
    const $accountSelect = $("#account-select");
    if ($accountSelect.length === 0) {
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
            $accountSelect.html('<option value="">Failed to load accounts</option>');
            return;
        }

        const data = await resp.json();
        console.log("[loadAccounts] data:", data);

        const accounts = data.accounts || [];

        // Clear and populate dropdown using jQuery
        $accountSelect.html('<option value="">-- Select an account --</option>');
        accounts.forEach(acc => {
            const email = acc.email;
            const name = acc.name || email;

            $accountSelect.append($('<option>', {
                value: email,                          
                text: `${name} (${email})`             
            }));
        });

        console.log("[loadAccounts] populated with", accounts.length, "accounts");
    } catch (err) {
        console.error("[loadAccounts] error:", err);
        $accountSelect.html('<option value="">Error loading accounts</option>');
    }
}

/**
 * Handle account selection change
 */
function handleAccountChange(event) {
    selectedAccount = $(event.target).val();
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
    const $step2 = $("#step-2");
    if ($step2.length === 0) return;

    $step2.show().addClass("fade-in");

    console.log("[showStep2] Step 2 displayed");
}

function handleFileSelect(event) {
    const file = event.target.files[0];
    if (!file) return;

    console.log("[handleFileSelect] file selected:", file.name);
    uploadedFile = file;
    updateFileUI(file);
}

function removeFile() {
    console.log("[removeFile] removing file");
    uploadedFile = null;

    $("#excel-file-input").val("");
    $("#file-info-row").hide();
    $(".upload-label").show();
    $("#generate-preview-btn").hide();
}

/**
 * Handle drag and drop events for file upload
 */
function initializeDragAndDrop() {
    const $uploadArea = $('.upload-area');
    if ($uploadArea.length === 0) {
        console.warn('[initializeDragAndDrop] upload-area not found');
        return;
    }

    // Prevent default drag behaviors
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        $uploadArea.on(eventName, preventDefaults);
        $('body').on(eventName, preventDefaults);
    });

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    // Highlight drop area when item is dragged over it
    $uploadArea.on('dragenter dragover', function () {
        $(this).addClass('drag-over');
    });

    $uploadArea.on('dragleave drop', function () {
        $(this).removeClass('drag-over');
    });

    // Handle dropped files
    $uploadArea.on('drop', function (e) {
        const dt = e.originalEvent.dataTransfer;
        const files = dt.files;

        if (files.length > 0) {
            const file = files[0];

            // Validate file type
            const validExtensions = ['.xlsx', '.xls'];
            const fileName = file.name.toLowerCase();
            const isValid = validExtensions.some(ext => fileName.endsWith(ext));

            if (!isValid) {
                alert('Please upload an Excel file (.xlsx or .xls)');
                return;
            }

            // Set the file and update UI
            uploadedFile = file;
            updateFileUI(file);
            console.log('[handleDrop] file dropped:', file.name);
        }
    });
}

/**
 * Update UI after file is selected (used by both click and drag-drop)
 */
function updateFileUI(file) {
    $("#file-name").text(file.name);
    $(".upload-label").hide();
    $("#file-info-row").css('display', 'flex');
    $("#generate-preview-btn").show();
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
        const $a = $('<a>', {
            href: url,
            download: "campaign_sheet_template.xlsx"
        });
        $('body').append($a);
        $a[0].click();
        window.URL.revokeObjectURL(url);
        $a.remove();

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

    // Show loading state
    const $modal = $('#previewModal');
    const $loading = $('#preview-loading');
    const $table = $('#preview-table');
    const $info = $('#preview-info');

    // Show modal
    const modal = new bootstrap.Modal($modal[0]);
    modal.show();

    // Show loading spinner
    $loading.show();
    $table.parent().hide();
    $info.hide();

    try {
        // Create FormData to send file
        const formData = new FormData();
        formData.append('sheet', uploadedFile);

        // Call API
        const resp = await fetch('/api/upload-preview', {
            method: 'POST',
            body: formData
        });

        if (!resp.ok) {
            const errorData = await resp.json();
            throw new Error(errorData.error || 'Failed to load preview');
        }

        const data = await resp.json();
        console.log("[generatePreview] preview data:", data);

        // Populate modal with data
        populatePreviewModal(data.preview, uploadedFile.name);

        // Hide loading, show table
        $loading.hide();
        $table.parent().show();
        $info.show();

    } catch (err) {
        console.error("[generatePreview] error:", err);
        modal.hide();
        alert("Error loading preview: " + err.message);
    }
}

/**
 * Populate preview modal with Excel data
 */
function populatePreviewModal(preview, filename) {
    // Set file info
    $('#preview-filename').text(filename);
    $('#preview-total-rows').text(preview.total_rows || 0);
    $('#preview-showing-rows').text(preview.preview_count || 0);

    const $table = $('#preview-table');
    const columns = preview.columns || [];
    const rows = preview.rows || [];

    // Build table header
    let headerHtml = '<tr>';
    columns.forEach(col => {
        headerHtml += `<th>${escapeHtml(col)}</th>`;
    });
    headerHtml += '</tr>';
    $table.find('thead').html(headerHtml);

    // Build table body
    let bodyHtml = '';
    rows.forEach((row, index) => {
        bodyHtml += '<tr>';
        columns.forEach(col => {
            const value = row[col] !== undefined ? row[col] : '';
            const valueStr = String(value);
            bodyHtml += `<td title="${escapeAttr(valueStr)}">${escapeHtml(valueStr)}</td>`;
        });
        bodyHtml += '</tr>';
    });
    $table.find('tbody').html(bodyHtml);
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Escape text for use in HTML attributes
 */
function escapeAttr(text) {
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

/**
 * Sync uploaded file to Broadciel via /api/commit
 */
async function syncToCommit() {
    console.log('[syncToCommit] starting sync');

    // Validation
    if (!uploadedFile) {
        throw new Error('No file uploaded');
    }

    if (!selectedAccount) {
        throw new Error('No account selected');
    }

    // Create FormData with file and account_email
    const formData = new FormData();
    formData.append('file', uploadedFile);
    formData.append('account_email', selectedAccount);

    console.log('[syncToCommit] uploading file:', uploadedFile.name, 'for account:', selectedAccount);

    // Call /api/commit endpoint
    const response = await fetch('/api/commit', {
        method: 'POST',
        body: formData
    });

    console.log('[syncToCommit] /api/commit status:', response.status);

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || 'Sync failed');
    }

    const result = await response.json();
    console.log('[syncToCommit] result:', result);

    return result;
}

/**
 * Show a custom toast notification
 */
function showToast(message, type = 'info') {
    const $container = $('#toast-container');

    // Create toast element
    const $toast = $('<div>', {
        class: `custom-toast ${type}`,
        html: `
            <div class="toast-message">${escapeHtml(message)}</div>
            <button class="toast-close">&times;</button>
        `
    });

    // Add close handler
    $toast.find('.toast-close').on('click', function () {
        hideToast($toast);
    });

    // Add to container
    $container.append($toast);

    // Auto remove after 5 seconds
    setTimeout(() => {
        hideToast($toast);
    }, 5000);
}

function hideToast($toast) {
    if ($toast.hasClass('hiding')) return;
    $toast.addClass('hiding');
    $toast.on('animationend', function () {
        $toast.remove();
    });
}

/**
 * Handle Confirm & Sync button click
 */
async function handleConfirmSync() {
    console.log('[handleConfirmSync] button clicked');

    // Validation
    if (!uploadedFile) {
        showToast('No file uploaded. Please upload a file first.', 'error');
        return;
    }

    if (!selectedAccount) {
        showToast('Please select an account before syncing.', 'error');
        return;
    }

    const $btn = $('#confirm-preview-btn');
    const originalText = $btn.text();

    // Disable button and show loading state
    $btn.prop('disabled', true);
    $btn.text('Syncing...');

    try {
        await syncToCommit();

        // Use custom toast instead of alert
        showToast('Successfully synced to Broadciel!', 'success');

        // Close modal
        $('#previewModal').modal('hide');

        // === Reset Workflow ===
        // 1. Remove file
        removeFile();

        // 2. Reset Step 1 (Account Selection)
        selectedAccount = null;
        $('#account-select').val('');

        // 3. Hide Step 2
        $('#step-2').hide().removeClass('fade-in');

    } catch (error) {

        console.error('[handleConfirmSync] error:', error);
        // Use custom toast instead of alert
        showToast(`Sync failed: ${error.message}`, 'error');
    } finally {
        // Restore button state
        $btn.prop('disabled', false);
        $btn.text(originalText);
    }
}


// Initialize on page load using jQuery
$(document).ready(function () {
    console.log("[cmp.js] Document ready");

    loadAccounts();

    // Event handlers using jQuery
    $("#account-select").on("change", handleAccountChange);
    $("#download-template-btn").on("click", downloadTemplate);
    $("#excel-file-input").on("change", handleFileSelect);
    $("#remove-file-btn").on("click", removeFile);
    $("#generate-preview-btn").on("click", generatePreview);
    $("#confirm-preview-btn").on("click", handleConfirmSync);

    // Initialize drag and drop
    initializeDragAndDrop();
});

// Expose functions to window for compatibility
window.loadAccounts = loadAccounts;
window.downloadTemplate = downloadTemplate;
window.generatePreview = generatePreview;
window.syncToCommit = syncToCommit;
window.showToast = showToast;
