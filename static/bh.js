const { createApp, ref, onMounted, computed, watch, nextTick } = Vue;

console.log('bh.js loaded v1.3');

window.toggleLauncher = () => {
    const menu = document.getElementById('app-launcher-menu');
    menu.classList.toggle('show');

    // Close when clicking outside
    if (menu.classList.contains('show')) {
        const closeHandler = (e) => {
            if (!e.target.closest('.app-launcher-container')) {
                menu.classList.remove('show');
                document.removeEventListener('click', closeHandler);
            }
        };
        setTimeout(() => document.addEventListener('click', closeHandler), 0);
    }
};

const app = createApp({
    delimiters: ['[[', ']]'],
    setup() {
        console.log('bh.js setup started');
        // --- State ---


        const accounts = ref([]);
        const loading = ref(false);
        const searchQuery = ref('');
        const searchScope = ref('mine'); // 'mine' or 'all'

        // Bulk Selection
        const selectedAccountIds = ref([]);


        // Drawer State
        const isDrawerOpen = ref(false);
        const selectedAccount = ref(null);

        const dailyStats = ref([]);

        // Upload State
        const uploadFile = ref(null);
        const isUploading = ref(false);

        // Sync State
        const isSyncing = ref(false);
        const syncLogs = ref([]);
        const syncModalInstance = ref(null);

        // --- Computed ---
        const isAllSelected = computed(() => {
            return accounts.value.length > 0 && selectedAccountIds.value.length === accounts.value.length;
        });



        // --- Methods ---



        const loadAccounts = async () => {
            loading.value = true;
            try {
                const params = new URLSearchParams({
                    search: searchQuery.value,
                    scope: searchScope.value
                });
                const res = await fetch(`/api/bh/accounts?${params}`);
                const data = await res.json();
                if (data.status === 'ok') {
                    accounts.value = data.accounts;
                    selectedAccountIds.value = []; // Clear selection on reload
                }

            } catch (e) {
                console.error('Failed to load accounts', e);
            } finally {
                loading.value = false;
            }
        };

        // Debounced Search
        let searchTimeout;
        const onSearchInput = () => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                loadAccounts();
            }, 300);
        };

        watch(searchScope, () => {
            loadAccounts();
        });

        const loadDailyStats = async (accountId) => {
            dailyStats.value = [];
            try {
                const res = await fetch(`/api/bh/account/${accountId}/daily`);
                const data = await res.json();
                if (data.status === 'ok') {
                    dailyStats.value = data.stats;
                }
            } catch (e) {
                console.error('Failed to load daily stats', e);
            }
        };

        const openDrawer = (account) => {
            selectedAccount.value = { ...account }; // Clone
            isDrawerOpen.value = true;
            loadDailyStats(account.account_id);
        };

        const closeDrawer = () => {
            isDrawerOpen.value = false;
            selectedAccount.value = null;
            dailyStats.value = [];
        };

        const saveAccount = async () => {
            if (!isDrawerOpen.value || !selectedAccount.value) return;

            try {
                const res = await fetch(`/api/bh/account/${selectedAccount.value.account_id}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(selectedAccount.value)
                });
                const data = await res.json();
                if (data.status === 'ok') {
                    alert('Saved Successfully');
                    closeDrawer();
                    loadAccounts(); // Refresh list to update budget/goals

                } else {
                    alert('Save Failed: ' + (data.message || 'Unknown error'));
                }
            } catch (e) {
                alert('Save Error: ' + e.message);
            }
        };

        const triggerUpload = () => {
            document.getElementById('excel-upload-input').click();
        };

        const handleFileUpload = async (event) => {
            const file = event.target.files[0];
            if (!file) return;

            isUploading.value = true;
            const formData = new FormData();
            formData.append('file', file);

            try {
                const res = await fetch('/api/bh/upload', {
                    method: 'POST',
                    body: formData
                });
                const data = await res.json();
                if (data.status === 'ok') {
                    alert(`Upload Successful! Inserted: ${data.result.inserted}, Errors: ${data.result.errors.length}`);
                    loadAccounts();

                } else {
                    alert('Upload Failed: ' + (data.message || 'Unknown error'));
                }
            } catch (e) {
                alert('Upload Error: ' + e.message);
            } finally {
                isUploading.value = false;
                event.target.value = ''; // Reset input
            }
        };

        const triggerSync = () => {
            if (isSyncing.value) return;
            isSyncing.value = true;
            syncLogs.value = [];

            // Connect SSE
            const eventSource = new EventSource('/api/bh/sync');

            // Prevent background scrolling
            document.body.style.overflow = 'hidden';

            eventSource.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    syncLogs.value.push(data);

                    // Auto-scroll scroll to bottom
                    nextTick(() => {
                        const term = document.getElementById('sync-console');
                        if (term) term.scrollTop = term.scrollHeight;
                    });

                    if (data.done) {
                        eventSource.close();
                        isSyncing.value = false;
                        // Refresh data
                        loadAccounts();
                    }
                    if (data.type === 'error') {
                        // Don't close immediately, let user see error
                    }
                } catch (e) {
                    console.error('Error parsing SSE', e);
                }
            };

            eventSource.onerror = (e) => {
                // ... error handling ...
                console.error('SSE Error', e);
                eventSource.close();
                isSyncing.value = false;
                syncLogs.value.push({ msg: 'Connection Closed.', type: 'info' });
            };
        };

        const closeSyncModal = () => {
            syncLogs.value = [];
            isSyncing.value = false;
            document.body.style.overflow = '';
        };

        const getProgressColor = (percent) => {
            if (percent < 95) return 'text-custom-red';
            if (percent < 100) return 'text-custom-orange';
            return 'text-custom-green';
        };

        const triggerDownload = async () => {
            try {
                const res = await fetch('/api/bh/download', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        search: searchQuery.value,
                        scope: searchScope.value
                    })
                });

                if (res.ok) {
                    const blob = await res.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `budget_hunter_export_${new Date().toISOString().split('T')[0]}.xlsx`;
                    document.body.appendChild(a);
                    a.click();
                    window.URL.revokeObjectURL(url);
                    document.body.removeChild(a);
                } else {
                    const data = await res.json();
                    alert('Download Failed: ' + (data.message || 'Unknown error'));
                }
            } catch (e) {
                console.error('Download error', e);
                alert('Download Error: ' + e.message);
            }
        };

        // Bulk Actions
        const toggleSelectAll = () => {
            if (isAllSelected.value) {
                selectedAccountIds.value = [];
            } else {
                selectedAccountIds.value = accounts.value.map(a => a.account_id);
            }
        };

        const clearSelection = () => {
            selectedAccountIds.value = [];
        };

        const toggleSelection = (accId) => {
            const idx = selectedAccountIds.value.indexOf(accId);
            if (idx > -1) {
                selectedAccountIds.value.splice(idx, 1);
            } else {
                selectedAccountIds.value.push(accId);
            }
        };

        const archiveSelected = async () => {
            if (selectedAccountIds.value.length === 0) return;

            if (!confirm(`Are you sure you want to archive ${selectedAccountIds.value.length} accounts?`)) return;

            try {
                const res = await fetch('/api/bh/accounts/bulk-status', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        account_ids: selectedAccountIds.value,
                        status: 'archived'
                    })
                });
                const data = await res.json();

                if (data.status === 'ok') {
                    alert(`Successfully archived ${data.updated_count} accounts.`);
                    loadAccounts(); // will clear selection too
                } else {
                    alert('Error: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Request failed: ' + e.message);
            }
        };

        // --- Lifecycle ---

        onMounted(() => {

            loadAccounts();
        });

        return {

            accounts,
            loading,
            searchQuery,
            searchScope,
            isDrawerOpen,
            selectedAccount,
            uploadFile,
            isUploading,
            isSyncing,
            syncLogs,
            dailyStats,

            loadAccounts,
            onSearchInput,
            openDrawer,
            closeDrawer,
            saveAccount,
            triggerUpload,
            handleFileUpload,
            triggerSync,
            closeSyncModal,
            getProgressColor,
            triggerDownload,
            formatDate: (d) => {
                if (!d) return '-';
                try {
                    return new Date(d).toLocaleDateString(undefined, { month: 'numeric', day: 'numeric' });
                } catch (e) { return d; }
            },
            formatNumber: (n, decimals = 0) => {
                if (n === undefined || n === null || isNaN(n)) return '0';
                try {
                    return Number(n).toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
                } catch (e) { return '0'; }
            },
            mathMin: (a, b) => Math.min(a, b),

            // Bulk
            selectedAccountIds,
            isAllSelected,
            toggleSelectAll,
            toggleSelection,
            clearSelection,
            archiveSelected
        };

    }
});

try {
    console.log('Mounting Vue app...');
    app.mount('#app');
    console.log('Vue app mounted successfully');
} catch (e) {
    console.error('Vue Mount Error:', e);
    alert('Vue Mount Error: ' + e.message);
}
