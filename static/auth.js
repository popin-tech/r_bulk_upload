console.log("[auth.js] Loaded");

document.addEventListener("DOMContentLoaded", async () => {
    if (!isLoginPage) {
        await checkSession();
    }
});

/**
 * Check if the user has an active session
 */
async function checkSession() {
    try {
        const resp = await fetch("/api/me");
        if (resp.ok) {
            const data = await resp.json();
            const email = data.user?.email;
            if (email) {
                console.log("[auth.js] Session valid for:", email);
                localStorage.setItem("user_email", email); // Cache for UI
                replaceGoogleButton(email);
                return data.user;
            }
        } else {
            if (isCmpPage) {
                console.warn("[auth.js] Session invalid on CMP page, redirecting...");
                window.location.href = "/login";
            }
        }
    } catch (err) {
        console.error("[auth.js] Session check failed:", err);
    }
    return null;
}

/**
 * Replace Google Sign-In button with logged-in state
 * @param {string} email - User's email address
 */
function replaceGoogleButton(email) {
    console.log("[replaceGoogleButton] for:", email);
    const container = document.getElementById("g_id_signin_container");
    if (!container) {
        return;
    }
    container.innerHTML = "";
    const logged = document.createElement("div");
    logged.className = "logged-in-button";
    logged.textContent = `Logged in as ${email} • Click to logout`;
    logged.addEventListener("click", () => {
        console.log("[replaceGoogleButton] logout clicked");
        logout();
    });
    container.appendChild(logged);
}

/**
 * Logout function - calls API to clear session
 */
async function logout() {
    console.log("[logout] calling /api/logout");
    try {
        await fetch("/api/logout", { method: "POST" });
    } catch (err) {
        console.error("[logout] API error:", err);
    }

    localStorage.removeItem("user_email");
    window.location.href = "/login";
}

/**
 * Handle Google Sign-In credential callback
 * Exchanges Google Token for Server-side Session
 */
async function handleGoogleCredential(response, options = {}) {
    console.log("[handleGoogleCredential] GIS callback received");
    const googleToken = response.credential;
    const userInfo = document.getElementById("user-info");

    try {
        console.log("[handleGoogleCredential] POST /api/login");
        const loginResp = await fetch("/api/login", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({ token: googleToken })
        });

        if (!loginResp.ok) {
            const text = await loginResp.text();
            console.error("[handleGoogleCredential] Login failed:", text);

            if (userInfo) {
                userInfo.textContent = "⚠️ Authentication failed. You may not be authorized.";
                userInfo.style.background = "rgba(244, 67, 54, 0.2)";
                userInfo.style.border = "1px solid rgba(244, 67, 54, 0.4)";
            }

            if (options.onError) options.onError(text);
            return;
        }

        const data = await loginResp.json();
        console.log("[handleGoogleCredential] Login success:", data);
        const email = data.user?.email || "";

        localStorage.setItem("user_email", email);

        if (userInfo) {
            userInfo.textContent = "";
            userInfo.style.background = "";
            userInfo.style.border = "";
        }

        // Update UI
        replaceGoogleButton(email);

        // Call success callback
        if (options.onSuccess) {
            options.onSuccess(data);
        }

    } catch (err) {
        console.error("[handleGoogleCredential] Error:", err);
        if (userInfo) {
            userInfo.textContent = "⚠️ System error. Please try again.";
        }
        if (options.onError) options.onError(err);
    }
}

const isLoginPage = window.location.pathname === '/login';
const isCmpPage = window.location.pathname === '/cmp';

const originalHandleGoogleCredential = handleGoogleCredential;

// Login Page Specific Logic
if (isLoginPage) {
    console.log("[auth.js] Login page setup");

    window.handleGoogleCredential = function (response) {
        const successMessage = document.getElementById("success-message");

        originalHandleGoogleCredential(response, {
            onSuccess: (data) => {
                console.log("[auth.js] Redirecting to CMP...");
                if (successMessage) successMessage.classList.add("show");

                setTimeout(() => {
                    window.location.href = "/cmp";
                }, 1500);
            },
            onError: (err) => {
                console.error("[auth.js] Login failed callback:", err);
            }
        });
    };
} else {
    window.handleGoogleCredential = originalHandleGoogleCredential;
}

window.replaceGoogleButton = replaceGoogleButton;
window.logout = logout;
window.checkSession = checkSession;
