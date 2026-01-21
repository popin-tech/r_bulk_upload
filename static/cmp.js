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
// Store last sync result for re-opening
let lastSyncResult = null;

function hideLastResultBtn() {
    $('#view-last-result-btn').hide();
    lastSyncResult = null;
}

function handleAccountChange(event) {
    selectedAccount = $(event.target).val();
    console.log("[handleAccountChange] selected:", selectedAccount);


    // Reset previous sync results on account change
    hideLastResultBtn();

    if (selectedAccount) {
        console.log("[handleAccountChange] Account selected, showing Step 2");
        showStep2();
        // Show download button
        $("#download-structure-btn").fadeIn();
    } else {
        $("#download-structure-btn").hide();
    }
}

/**
 * Handle Download Structure button click
 */
async function handleDownloadStructure() {
    console.log("[handleDownloadStructure] clicked");

    if (!selectedAccount) {
        showToast("Please select an account first", "error");
        return;
    }

    const $btn = $("#download-structure-btn");
    const originalText = $btn.html();

    // Disable button and show loading
    $btn.prop("disabled", true);
    $btn.html(`
        <span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
        Downloading...
    `);

    try {
        const resp = await fetch("/api/download-excel", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({ account_email: selectedAccount })
        });

        if (!resp.ok) {
            const errData = await resp.json();
            throw new Error(errData.error || "Download failed");
        }

        const data = await resp.json();
        if (data.status !== "ok" || !data.file_base64) {
            throw new Error(data.error || "Invalid response from server");
        }

        // Convert Base64 to Blob
        const byteCharacters = atob(data.file_base64);
        const byteNumbers = new Array(byteCharacters.length);
        for (let i = 0; i < byteCharacters.length; i++) {
            byteNumbers[i] = byteCharacters.charCodeAt(i);
        }
        const byteArray = new Uint8Array(byteNumbers);
        const blob = new Blob([byteArray], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });

        // Trigger download
        const url = window.URL.createObjectURL(blob);
        const $a = $('<a>', {
            href: url,
            download: data.filename || `structure_${selectedAccount}.xlsx`
        });
        $('body').append($a);
        $a[0].click();
        window.URL.revokeObjectURL(url);
        $a.remove();

        showToast("Structure downloaded successfully!", "success");

    } catch (err) {
        console.error("[handleDownloadStructure] error:", err);
        showToast(`Error downloading structure: ${err.message}`, "error");
    } finally {
        // Restore button
        $btn.prop("disabled", false);
        $btn.html(originalText);
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
    // Reset previous sync results on new file
    hideLastResultBtn();

    updateFileUI(file);
}

function removeFile() {
    console.log("[removeFile] removing file");
    uploadedFile = null;

    // Reset previous sync results
    hideLastResultBtn();

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
        const result = await syncToCommit();

        // Close preview modal
        $('#previewModal').modal('hide');

        // Save result and show button
        lastSyncResult = result;
        $('#view-last-result-btn').show();

        // Show Result Modal
        showResultModal(result);

        showToast('Successfully synced to Broadciel!', 'success');

    } catch (error) {
        console.error('[handleConfirmSync] error:', error);
        showToast(`Sync failed: ${error.message}`, 'error');
    } finally {
        // Restore button state
        $btn.prop('disabled', false);
        $btn.text(originalText);
    }
}

/**
 * Show the Result Modal with hierarchical data
 */
function showResultModal(commitResult) {
    const $modal = $('#resultModal');
    const $container = $('#result-tree');

    // Clear previous results
    $container.empty();

    const processingResult = commitResult.processing_result;
    if (!processingResult || !processingResult.details) {
        $container.html('<div class="text-white p-3">No detail results available.</div>');
    } else {
        // Render Tree
        renderSyncResults(processingResult.details, $container);
    }

    // Show modal
    const modal = new bootstrap.Modal($modal[0]);
    modal.show();

    // Bind close event for reset
    $('#result-modal-close-btn').off('click').on('click', closeResultModal);
    $('#result-modal-corner-close').off('click').on('click', closeResultModal);
}

/**
 * Recursive function to render result tree
 */
function renderSyncResults(items, $container) {
    if (!items || items.length === 0) return;

    items.forEach(item => {
        // Determine type based on properties
        let type = 'unknown';
        let name = '';
        let children = [];
        let success = item.success;
        let errorMsg = item.error_message;

        if (item.campaign_name !== undefined) {
            type = 'campaign';
            name = item.campaign_name || `Campaign #${item.campaign_index}`;
            children = item.ad_groups || [];
        } else if (item.ad_group_name !== undefined) {
            type = 'adgroup';
            name = item.ad_group_name || `AdGroup #${item.ad_group_index}`;
            children = item.ad_assets || [];
        } else if (item.creative_name !== undefined) {
            type = 'creative';
            name = item.creative_name || `Creative #${item.ad_asset_index}`;
        }

        // Build Node HTML
        const statusIcon = success ?
            '<i class="bi bi-check-circle-fill status-success"></i>' :
            '<i class="bi bi-x-circle-fill status-error"></i>';

        const $node = $(`
            <div class="result-node level-${type}">
                ${children.length > 0 ? '<div class="result-collapse"><i class="bi bi-caret-down-fill"></i></div>' : '<div class="result-collapse" style="opacity:0"></div>'}
                <div class="result-status-icon">${statusIcon}</div>
                <div class="result-content">
                    <div class="result-title">${escapeHtml(name)}</div>
                    ${errorMsg ? `<div class="result-error-msg">${escapeHtml(errorMsg)}</div>` : ''}
                </div>
            </div>
        `);

        // Handle Collapse/Expand
        if (children.length > 0) {
            $node.find('.result-collapse').on('click', function () {
                const $icon = $(this);
                const $childrenContainer = $node.next('.result-children');
                $childrenContainer.slideToggle(200);
                $icon.toggleClass('collapsed');
            });
        }

        $container.append($node);

        // Render Children
        if (children.length > 0) {
            const $childrenContainer = $('<div class="result-children"></div>');
            $container.append($childrenContainer);
            renderSyncResults(children, $childrenContainer);
        }
    });
}

function viewLastResult() {
    if (lastSyncResult) {
        showResultModal(lastSyncResult);
    } else {
        showToast('No previous result found', 'warning');
    }
}

function closeResultModal() {
    $('#resultModal').modal('hide');
    // Note: We NO LONGER reset the workflow here.
    // The user stays on Step 2 with the file selected and can view the result again.
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
    $("#view-last-result-btn").on("click", viewLastResult);
    $("#download-structure-btn").on("click", handleDownloadStructure);

    // Initialize drag and drop
    initializeDragAndDrop();
});

// Expose functions to window for compatibility
window.loadAccounts = loadAccounts;
window.downloadTemplate = downloadTemplate;
window.generatePreview = generatePreview;
window.syncToCommit = syncToCommit;
window.viewLastResult = viewLastResult;
window.handleDownloadStructure = handleDownloadStructure;
