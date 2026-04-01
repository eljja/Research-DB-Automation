document.addEventListener("DOMContentLoaded", () => {
    const days = [
        { key: "mon", label: "Mon" },
        { key: "tue", label: "Tue" },
        { key: "wed", label: "Wed" },
        { key: "thu", label: "Thu" },
        { key: "fri", label: "Fri" },
        { key: "sat", label: "Sat" },
        { key: "sun", label: "Sun" },
    ];

    const state = {
        topics: [],
        currentTopicId: null,
        currentPage: 1,
        chart: null,
        graphRows: [],
        graphColumns: [],
        debugEnabled: false,
        lastDebugId: 0,
    };

    const els = {
        topicSelect: document.getElementById("topic-select"),
        queryInput: document.getElementById("query-input"),
        scheduleInputs: document.getElementById("schedule-inputs"),
        saveConfigBtn: document.getElementById("save-config-btn"),
        senseBtn: document.getElementById("btn-sense"),
        senseOffset: document.getElementById("sense-offset"),
        abstractBtn: document.getElementById("btn-abstract"),
        abstractLimit: document.getElementById("abstract-limit"),
        llmBtn: document.getElementById("btn-llm"),
        llmLimit: document.getElementById("llm-limit"),
        notice: document.getElementById("notice"),
        kstTime: document.getElementById("kst-time"),
        logContainer: document.getElementById("log-container"),
        debugContainer: document.getElementById("debug-log-container"),
        debugToggleBtn: document.getElementById("btn-debug-toggle"),
        dbTableBody: document.querySelector("#db-table tbody"),
        prevPageBtn: document.getElementById("prev-page"),
        nextPageBtn: document.getElementById("next-page"),
        pageButtons: document.getElementById("page-buttons"),
        pageInfo: document.getElementById("page-info"),
        xAxis: document.getElementById("x-axis"),
        yAxis: document.getElementById("y-axis"),
        xLog: document.getElementById("x-log"),
        yLog: document.getElementById("y-log"),
        pointSize: document.getElementById("point-size"),
        resetZoomBtn: document.getElementById("btn-reset-zoom"),
        refreshGraphBtn: document.getElementById("btn-refresh-graph"),
        statTotal: document.getElementById("stat-total"),
        statNew: document.getElementById("stat-new"),
        statAbstract: document.getElementById("stat-abstract"),
        statLlm: document.getElementById("stat-llm"),
        statError: document.getElementById("stat-error"),
        statTopics: document.getElementById("stat-topics"),
        debugStateLabel: document.getElementById("debug-state-label"),
    };

    initScheduleInputs();
    bindEvents();
    bootstrap();

    function bindEvents() {
        els.topicSelect.addEventListener("change", () => {
            state.currentTopicId = Number(els.topicSelect.value);
            const topic = state.topics.find((item) => item.id === state.currentTopicId);
            if (topic) {
                populateTopic(topic);
                state.currentPage = 1;
                fetchPapers();
            }
        });

        els.saveConfigBtn.addEventListener("click", saveTopicConfig);
        els.senseBtn.addEventListener("click", () => {
            const start = Number.parseInt(els.senseOffset.value || "0", 10) || 0;
            triggerAction("/api/actions/sense", { topic_id: state.currentTopicId, start }, `Google sensing started at offset ${start}.`);
        });
        els.abstractBtn.addEventListener("click", () => {
            const limit = Number.parseInt(els.abstractLimit.value || "10", 10) || 10;
            triggerAction("/api/actions/fetch_abstracts", { limit }, `Abstract fetch started for ${limit} items.`);
        });
        els.llmBtn.addEventListener("click", () => {
            const limit = Number.parseInt(els.llmLimit.value || "10", 10) || 10;
            triggerAction("/api/actions/process_llm", { limit }, `LLM analysis started for ${limit} items.`);
        });
        els.prevPageBtn.addEventListener("click", () => {
            if (state.currentPage > 1) {
                state.currentPage -= 1;
                fetchPapers();
            }
        });
        els.nextPageBtn.addEventListener("click", () => {
            state.currentPage += 1;
            fetchPapers();
        });
        els.debugToggleBtn.addEventListener("click", toggleDebug);
        els.dbTableBody.addEventListener("click", handleTableClick);
        [els.xAxis, els.yAxis, els.xLog, els.yLog, els.pointSize].forEach((el) => el.addEventListener("change", updateGraph));
        els.resetZoomBtn.addEventListener("click", () => {
            if (state.chart) {
                state.chart.resetZoom();
            }
        });
        els.refreshGraphBtn.addEventListener("click", fetchGraphData);
    }

    async function bootstrap() {
        await Promise.all([fetchTopics(), fetchDashboard(), fetchLogs(), fetchDebugSetting()]);
        await Promise.all([fetchPapers(), fetchGraphData()]);
        setInterval(fetchDashboard, 10000);
        setInterval(fetchLogs, 4000);
        setInterval(fetchDebugLogs, 2000);
    }

    function initScheduleInputs() {
        els.scheduleInputs.innerHTML = "";
        days.forEach((day) => {
            const card = document.createElement("div");
            card.className = "schedule-card";
            card.innerHTML = `
                <div class="schedule-top">
                    <strong>${day.label}</strong>
                    <label class="toggle">
                        <input type="checkbox" id="check-${day.key}">
                        <span>Run</span>
                    </label>
                </div>
                <div class="time-selects">
                    <select id="hour-${day.key}">${buildHour24Options()}</select>
                    <span>:00</span>
                </div>
            `;
            els.scheduleInputs.appendChild(card);
        });
    }

    function initAxisSelects(columns) {
        state.graphColumns = columns || [];
        els.xAxis.innerHTML = "";
        els.yAxis.innerHTML = "";
        state.graphColumns.forEach((item) => {
            els.xAxis.add(new Option(item.label, item.key));
            els.yAxis.add(new Option(item.label, item.key));
        });
        els.xAxis.value = "year";
        els.yAxis.value = state.graphColumns.some((item) => item.key === "endurance_cycles") ? "endurance_cycles" : "year";
    }

    async function fetchJson(url, options) {
        const response = await fetch(url, options);
        if (!response.ok) {
            throw new Error(`${response.status} ${response.statusText}`);
        }
        return response.json();
    }

    async function fetchTopics() {
        state.topics = await fetchJson("/api/topics");
        els.topicSelect.innerHTML = "";
        state.topics.forEach((topic) => {
            els.topicSelect.add(new Option(topic.name, topic.id));
        });

        const nvmTopic = state.topics.find((topic) => topic.name === "NVM");
        const initialTopic = nvmTopic || state.topics[0];
        if (initialTopic) {
            state.currentTopicId = initialTopic.id;
            els.topicSelect.value = String(initialTopic.id);
            populateTopic(initialTopic);
        }
    }

    function populateTopic(topic) {
        els.queryInput.value = topic.query || "";
        days.forEach((day) => {
            document.getElementById(`check-${day.key}`).checked = Boolean(topic[`${day.key}_enabled`]);
            document.getElementById(`hour-${day.key}`).value = fromTwentyFourHourHourOnly(topic[`${day.key}_time`] || "");
        });
    }

    async function saveTopicConfig() {
        const payload = { query: els.queryInput.value.trim() };
        days.forEach((day) => {
            payload[`${day.key}_enabled`] = document.getElementById(`check-${day.key}`).checked;
            payload[`${day.key}_time`] = `${document.getElementById(`hour-${day.key}`).value || "00"}:00`;
        });
        await fetchJson(`/api/topics/${state.currentTopicId}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        showNotice("Topic settings saved.");
        fetchDashboard();
        fetchLogs();
    }

    async function triggerAction(url, payload, message) {
        await fetchJson(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        showNotice(message);
        setTimeout(() => {
            fetchDashboard();
            fetchLogs();
            fetchPapers();
            fetchGraphData();
        }, 1500);
    }

    async function fetchDashboard() {
        const data = await fetchJson("/api/dashboard");
        els.statTotal.textContent = data.counts.total;
        els.statNew.textContent = data.counts.new;
        els.statAbstract.textContent = data.counts.abstract_fetched;
        els.statLlm.textContent = data.counts.llm_processed;
        els.statError.textContent = data.counts.error;
        els.statTopics.textContent = data.topic_count;
        els.kstTime.textContent = `KST ${formatTime(data.time_kst)}`;
    }

    async function fetchDebugSetting() {
        const data = await fetchJson("/api/settings/debug");
        state.debugEnabled = Boolean(data.debug_enabled);
        syncDebugUi();
    }

    async function toggleDebug() {
        const nextValue = !state.debugEnabled;
        const data = await fetchJson("/api/settings/debug", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ enabled: nextValue }),
        });
        state.debugEnabled = Boolean(data.debug_enabled);
        if (!state.debugEnabled) {
            state.lastDebugId = 0;
            els.debugContainer.innerHTML = "";
        }
        syncDebugUi();
        showNotice(`Debug logging ${state.debugEnabled ? "enabled" : "disabled"}.`);
        fetchLogs();
    }

    function syncDebugUi() {
        els.debugContainer.classList.toggle("hidden", !state.debugEnabled);
        els.debugToggleBtn.textContent = state.debugEnabled ? "Debug ON" : "Debug OFF";
        els.debugStateLabel.textContent = state.debugEnabled ? "ON" : "OFF";
    }

    async function fetchLogs() {
        const logs = await fetchJson("/api/logs?limit=40");
        els.logContainer.innerHTML = "";
        logs.forEach((log) => {
            const row = document.createElement("div");
            row.className = `log-row ${String(log.level || "").toLowerCase()}`;
            const detailText = [log.level, log.timestamp, log.message, log.raw_data].filter(Boolean).join("\n");
            row.dataset.tooltip = detailText;
            const oneLine = `${capitalize(String(log.level || "").toLowerCase())} - ${String(log.message || "").replace(/\s+/g, " ").trim()}`;
            row.innerHTML = `
                <div class="log-time">${formatLogTime(log.timestamp)}</div>
                <div>
                    <p>${escapeHtml(oneLine)}</p>
                </div>
            `;
            els.logContainer.appendChild(row);
        });
    }

    async function fetchDebugLogs() {
        if (!state.debugEnabled) {
            return;
        }

        const logs = await fetchJson(`/api/logs?debug=true&since_id=${state.lastDebugId}&limit=40`);
        logs.forEach((log) => {
            const card = document.createElement("div");
            card.className = "debug-row";
            const raw = escapeHtml(log.raw_data || "");
            card.innerHTML = `
                <div class="debug-meta">[${formatLogTime(log.timestamp)}] ${escapeHtml(log.message || "")}</div>
                <pre>${raw || "No raw_data"}</pre>
            `;
            els.debugContainer.appendChild(card);
            state.lastDebugId = Math.max(state.lastDebugId, Number(log.id || 0));
        });
        if (logs.length > 0) {
            els.debugContainer.scrollTop = els.debugContainer.scrollHeight;
        }
    }

    async function fetchPapers() {
        const query = state.currentTopicId ? `?page=${state.currentPage}&topic_id=${state.currentTopicId}` : `?page=${state.currentPage}`;
        const data = await fetchJson(`/api/papers${query}`);
        els.dbTableBody.innerHTML = "";

        if (data.page > data.pages) {
            state.currentPage = data.pages;
            return fetchPapers();
        }

        data.papers.forEach((paper) => {
            const row = document.createElement("tr");
            const effectiveStatus = Number(paper.excluded || 0) === 1 ? "excluded" : (paper.status || "");
            if (effectiveStatus === "excluded") {
                row.classList.add("excluded-row");
            }
            const statusLabel = formatStatusLabel(effectiveStatus);
            const statusTooltip = effectiveStatus === "abstract_fetched"
                ? escapeHtml((paper.abstract || "No abstract available").slice(0, 4000))
                : effectiveStatus === "abstract_error"
                    ? "Click to reset this item to new."
                    : "";
            const statusElement = (effectiveStatus === "abstract_error" || effectiveStatus === "abstract_fetched" || effectiveStatus === "llm_processed" || effectiveStatus === "excluded")
                ? `<button class="status-badge status-${effectiveStatus} status-button reset-status-button" data-result-id="${escapeHtml(paper.result_id)}" ${statusTooltip ? `data-tooltip="${statusTooltip}"` : ""}>${escapeHtml(statusLabel)}</button>`
                : `<span class="status-badge status-${effectiveStatus}" ${statusTooltip ? `data-tooltip="${statusTooltip}"` : ""}>${escapeHtml(statusLabel)}</span>`;
            row.innerHTML = `
                <td>
                    <div class="title-cell title-cell-actions">
                        <div class="title-main">
                            <button class="mini-action-btn delete-btn" data-result-id="${escapeHtml(paper.result_id)}" title="Delete from DB">X</button>
                            <a href="${paper.link || "#"}" target="_blank" rel="noreferrer">${escapeHtml(paper.title || "Untitled")}</a>
                        </div>
                        <div class="row-actions">
                            <button class="mini-action-btn exclude-btn" data-result-id="${escapeHtml(paper.result_id)}" title="Keep in DB only">👁</button>
                            <button class="mini-action-btn input-btn" data-result-id="${escapeHtml(paper.result_id)}" title="Paste abstract manually">Paste</button>
                        </div>
                    </div>
                </td>
                <td>${statusElement}</td>
                <td>${escapeHtml(paper.year_month || (paper.year ? String(paper.year).slice(0, 4) : ""))}</td>
                <td>${escapeHtml(paper.mechanism || "")}</td>
                <td>${escapeHtml(paper.architecture || "")}</td>
                <td>${escapeHtml(paper.stack || "")}</td>
                <td>${escapeHtml(paper.memory_window || "")}</td>
                <td>${escapeHtml(paper.voltage || "")}</td>
                <td>${escapeHtml(paper.speed || "")}</td>
                <td>${escapeHtml(paper.retention || "")}</td>
                <td>${escapeHtml(paper.endurance || "")}</td>
                <td>${escapeHtml(paper.uniqueness || "")}</td>
            `;
            els.dbTableBody.appendChild(row);
        });

        els.pageInfo.textContent = `Page ${data.page} / ${data.pages}`;
        els.prevPageBtn.disabled = data.page <= 1;
        els.nextPageBtn.disabled = data.page >= data.pages;
        renderPageButtons(data.page, data.pages);
    }

    async function handleTableClick(event) {
        const resetButton = event.target.closest(".reset-status-button");
        if (resetButton) {
            const resultId = resetButton.dataset.resultId;
            if (!resultId) {
                return;
            }

            await fetchJson(`/api/papers/${resultId}/reset_fetch_state`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
            });
            showNotice("Fetch state reset to new.");
            fetchDashboard();
            fetchPapers();
            return;
        }

        const deleteButton = event.target.closest(".delete-btn");
        if (deleteButton) {
            const resultId = deleteButton.dataset.resultId;
            if (!resultId) {
                return;
            }
            await fetchJson(`/api/papers/${resultId}`, { method: "DELETE" });
            showNotice("Paper deleted from DB.");
            fetchDashboard();
            fetchPapers();
            fetchGraphData();
            return;
        }

        const excludeButton = event.target.closest(".exclude-btn");
        if (excludeButton) {
            const resultId = excludeButton.dataset.resultId;
            if (!resultId) {
                return;
            }
            await fetchJson(`/api/papers/${resultId}/exclude`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
            });
            showNotice("Paper excluded from automation.");
            fetchDashboard();
            fetchPapers();
            return;
        }

        const inputButton = event.target.closest(".input-btn");
        if (inputButton) {
            const resultId = inputButton.dataset.resultId;
            if (!resultId) {
                return;
            }
            const abstract = window.prompt("Paste abstract text:");
            if (abstract === null) {
                return;
            }
            await fetchJson(`/api/papers/${resultId}/manual_abstract`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ abstract }),
            });
            showNotice("Manual abstract saved.");
            fetchDashboard();
            fetchPapers();
        }
    }

    async function fetchGraphData() {
        const data = await fetchJson("/api/papers/all");
        state.graphRows = data.papers || [];
        initAxisSelects(data.columns || []);
        updateGraph();
    }

    function updateGraph() {
        const xKey = els.xAxis.value;
        const yKey = els.yAxis.value;
        const xLog = els.xLog.checked;
        const yLog = els.yLog.checked;

        const points = state.graphRows
            .map((paper) => {
                const x = parseNumericValue(paper[xKey]);
                const y = parseNumericValue(paper[yKey]);
                return { x, y, raw: paper };
            })
            .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y))
            .filter((point) => (!xLog || point.x > 0) && (!yLog || point.y > 0));

        const ctx = document.getElementById("dbChart").getContext("2d");
        if (state.chart) {
            state.chart.destroy();
        }

        state.chart = new Chart(ctx, {
            type: "scatter",
            data: {
                datasets: [
                    {
                        label: `${labelForKey(yKey)} vs ${labelForKey(xKey)}`,
                        data: points,
                        backgroundColor: "rgba(21, 184, 166, 0.72)",
                        borderColor: "rgba(255, 182, 72, 0.9)",
                        borderWidth: 1.5,
                        pointRadius: Number.parseInt(els.pointSize.value || "5", 10),
                        pointHoverRadius: Number.parseInt(els.pointSize.value || "5", 10) + 2,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                scales: {
                    x: {
                        type: xLog ? "logarithmic" : "linear",
                        title: { display: true, text: labelForKey(xKey), color: "#f7f3e9" },
                        grid: { color: "rgba(255,255,255,0.08)" },
                        ticks: { color: "rgba(247,243,233,0.72)" },
                    },
                    y: {
                        type: yLog ? "logarithmic" : "linear",
                        title: { display: true, text: labelForKey(yKey), color: "#f7f3e9" },
                        grid: { color: "rgba(255,255,255,0.08)" },
                        ticks: { color: "rgba(247,243,233,0.72)" },
                    },
                },
                plugins: {
                    legend: {
                        labels: { color: "#f7f3e9" },
                    },
                    tooltip: {
                        callbacks: {
                            label(context) {
                                const raw = context.raw.raw;
                                return `${raw.category || "Other"} | ${raw.title || ""} | x=${context.raw.x}, y=${context.raw.y}`;
                            },
                        },
                    },
                    zoom: {
                        pan: { enabled: true, mode: "xy" },
                        zoom: {
                            wheel: { enabled: true },
                            pinch: { enabled: true },
                            mode: "xy",
                        },
                    },
                },
            },
        });
    }

    function renderPageButtons(currentPage, totalPages) {
        els.pageButtons.innerHTML = "";
        const startPage = Math.max(1, currentPage - 5);
        const endPage = Math.min(totalPages, currentPage + 5);

        for (let page = startPage; page <= endPage; page += 1) {
            const button = document.createElement("button");
            button.type = "button";
            button.className = `btn page-number-btn${page === currentPage ? " active" : ""}`;
            button.textContent = String(page);
            button.disabled = page === currentPage;
            button.addEventListener("click", () => {
                state.currentPage = page;
                fetchPapers();
            });
            els.pageButtons.appendChild(button);
        }
    }

    function parseNumericValue(value) {
        if (typeof value === "number") {
            return value;
        }
        if (!value) {
            return NaN;
        }
        const normalized = String(value)
            .replace(/,/g, "")
            .replace(/\s+/g, " ")
            .trim();

        const sci = normalized.match(/[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?/);
        return sci ? Number.parseFloat(sci[0]) : NaN;
    }

    function labelForKey(key) {
        return state.graphColumns.find((item) => item.key === key)?.label || key;
    }

    function formatTime(isoText) {
        if (!isoText) {
            return "--:--";
        }
        const date = new Date(isoText);
        if (Number.isNaN(date.getTime())) {
            return isoText;
        }
        return date.toLocaleString("en-US", {
            hour12: true,
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
        });
    }

    function formatLogTime(isoText) {
        if (!isoText) {
            return "--/--, --:--";
        }
        const date = new Date(isoText);
        if (Number.isNaN(date.getTime())) {
            return isoText;
        }
        return date.toLocaleString("en-US", {
            hour12: false,
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
        }).replace(",", ", ");
    }

    function buildHour24Options() {
        return Array.from({ length: 24 }, (_, idx) => {
            const value = String(idx).padStart(2, "0");
            return `<option value="${value}">${value}</option>`;
        }).join("");
    }

    function fromTwentyFourHourHourOnly(value) {
        if (!value || !value.includes(":")) {
            return "00";
        }
        const [hh] = value.split(":");
        return String(hh || "00").padStart(2, "0");
    }

    function showNotice(message) {
        els.notice.textContent = message;
        els.notice.classList.add("visible");
        window.clearTimeout(showNotice.timer);
        showNotice.timer = window.setTimeout(() => {
            els.notice.classList.remove("visible");
        }, 2800);
    }

    function escapeHtml(value) {
        return String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#39;");
    }

    function capitalize(value) {
        if (!value) {
            return "";
        }
        return value.charAt(0).toUpperCase() + value.slice(1);
    }

    function formatStatusLabel(status) {
        if (status === "abstract_error") {
            return "ERROR";
        }
        if (status === "abstract_fetched") {
            return "Fetched";
        }
        if (status === "llm_processed") {
            return "LLM";
        }
        if (status === "excluded") {
            return "Excluded";
        }
        return String(status || "")
            .split("_")
            .map((part) => capitalize(part))
            .join(" ");
    }
});
