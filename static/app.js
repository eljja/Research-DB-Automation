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
        sortBy: "created_at",
        sortDir: "desc",
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
        googleSenseBtn: document.getElementById("btn-google-sense"),
        senseLimit: document.getElementById("sense-limit"),
        senseOffset: document.getElementById("sense-offset"),
        abstractBtn: document.getElementById("btn-abstract"),
        fullPaperBtn: document.getElementById("btn-full-paper"),
        abstractLimit: document.getElementById("abstract-limit"),
        llmBtn: document.getElementById("btn-llm"),
        backfillBtn: document.getElementById("btn-backfill"),
        backfillPending: document.getElementById("backfill-pending"),
        llmLimit: document.getElementById("llm-limit"),
        notice: document.getElementById("notice"),
        kstTime: document.getElementById("kst-time"),
        logContainer: document.getElementById("log-container"),
        debugContainer: document.getElementById("debug-log-container"),
        debugToggleBtn: document.getElementById("btn-debug-toggle"),
        dbTableHead: document.querySelector("#db-table thead"),
        dbTableBody: document.querySelector("#db-table tbody"),
        prevPageBtn: document.getElementById("prev-page"),
        nextPageBtn: document.getElementById("next-page"),
        pageButtons: document.getElementById("page-buttons"),
        pageInfo: document.getElementById("page-info"),
        chartMode: document.getElementById("chart-mode"),
        categoryFieldWrap: document.getElementById("category-field-wrap"),
        categoryField: document.getElementById("category-field"),
        scatterControls: document.getElementById("scatter-controls"),
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
        els.senseBtn.addEventListener("click", async () => {
            const start = Math.max(0, Number.parseInt(els.senseOffset.value, 10) || 0);
            const total = Math.min(200, Math.max(20, Number.parseInt(els.senseLimit.value, 10) || 20));
            const batches = Math.ceil(total / 20);
            els.senseBtn.disabled = true;
            try {
                for (let i = 0; i < batches; i++) {
                    const batchStart = start + i * 20;
                    showNotice(`Sensing [${i + 1}/${batches}] offset ${batchStart}…`);
                    await fetchJson("/api/actions/sense", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ topic_id: state.currentTopicId, start: batchStart }),
                    });
                    fetchLogs();
                }
                showNotice(`Sensing complete: ${batches} batch(es) from offset ${start}.`);
            } catch (err) {
                showNotice(`Sensing error: ${err.message}`);
            } finally {
                els.senseBtn.disabled = false;
                fetchDashboard();
                fetchLogs();
                fetchPapers();
                fetchGraphData();
            }
        });
        els.googleSenseBtn.addEventListener("click", () => {
            showNotice("Google Sensing is not implemented yet.");
        });
        els.abstractBtn.addEventListener("click", () => {
            const limit = Number.parseInt(els.abstractLimit.value || "20", 10) || 20;
            triggerAction("/api/actions/fetch_abstracts", { limit, topic_id: state.currentTopicId }, `Abstract fetch started for ${limit} items.`);
        });
        els.fullPaperBtn.addEventListener("click", () => {
            const limit = Number.parseInt(els.abstractLimit.value || "20", 10) || 20;
            triggerAction("/api/actions/fetch_full_papers", { limit, topic_id: state.currentTopicId }, `Full paper fetch started for ${limit} items.`);
        });
        els.llmBtn.addEventListener("click", () => {
            const limit = Number.parseInt(els.llmLimit.value || "20", 10) || 20;
            triggerAction("/api/actions/process_llm", { limit, topic_id: state.currentTopicId }, `LLM analysis started for ${limit} items.`);
        });
        els.backfillBtn.addEventListener("click", async () => {
            const limit = Number.parseInt(els.llmLimit.value || "20", 10) || 20;
            await triggerAction("/api/actions/process_llm", { limit, topic_id: null }, `Backfill started for up to ${limit} items.`);
            fetchBackfillStatus();
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
        els.dbTableHead.addEventListener("click", handleTableHeaderClick);
        els.dbTableBody.addEventListener("click", handleTableClick);
        [els.chartMode, els.categoryField, els.xLog, els.yLog, els.pointSize].forEach((el) => el.addEventListener("change", updateGraph));
        [els.xAxis, els.yAxis].forEach((el) => el.addEventListener("change", () => {
            syncAxisLogDefaults();
            updateGraph();
        }));
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
        fetchBackfillStatus();
        setInterval(fetchDashboard, 10000);
        setInterval(fetchLogs, 4000);
        setInterval(fetchDebugLogs, 2000);
        setInterval(fetchBackfillStatus, 30000);
    }

    async function fetchBackfillStatus() {
        try {
            const data = await fetchJson("/api/actions/backfill_status");
            const pending = data.pending ?? 0;
            els.backfillPending.textContent = String(pending);
            els.backfillBtn.disabled = pending === 0;
        } catch (_) {
            els.backfillPending.textContent = "?";
        }
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
        syncAxisLogDefaults();
    }

    function syncAxisLogDefaults() {
        const logKeys = new Set(["speed_seconds", "endurance_cycles"]);
        els.xLog.checked = logKeys.has(els.xAxis.value);
        els.yLog.checked = logKeys.has(els.yAxis.value);
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
        const params = new URLSearchParams({
            page: String(state.currentPage),
            sort_by: state.sortBy,
            sort_dir: state.sortDir,
        });
        if (state.currentTopicId) {
            params.set("topic_id", String(state.currentTopicId));
        }
        const query = `?${params.toString()}`;
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
                <td>${escapeHtml(paper.key_material || "")}</td>
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
        state.sortBy = data.sort_by || state.sortBy;
        state.sortDir = data.sort_dir || state.sortDir;
        syncTableSortHeaders();
        renderPageButtons(data.page, data.pages);
    }

    function handleTableHeaderClick(event) {
        const header = event.target.closest(".sortable-header");
        if (!header) {
            return;
        }
        const nextSortBy = header.dataset.sortKey;
        if (!nextSortBy) {
            return;
        }

        if (state.sortBy === nextSortBy) {
            state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
        } else {
            state.sortBy = nextSortBy;
            state.sortDir = nextSortBy === "title" ? "asc" : "desc";
        }
        state.currentPage = 1;
        fetchPapers();
    }

    function syncTableSortHeaders() {
        els.dbTableHead.querySelectorAll(".sortable-header").forEach((header) => {
            const sortKey = header.dataset.sortKey;
            header.classList.remove("sort-asc", "sort-desc", "active");
            const baseLabel = header.textContent.replace(/\s+[▲▼]$/, "");
            if (header.dataset.baseLabel) {
                header.textContent = header.dataset.baseLabel;
            } else {
                header.dataset.baseLabel = baseLabel;
                header.textContent = baseLabel;
            }

            if (sortKey === state.sortBy) {
                header.classList.add("active", state.sortDir === "asc" ? "sort-asc" : "sort-desc");
                header.textContent = `${header.dataset.baseLabel} ${state.sortDir === "asc" ? "▲" : "▼"}`;
            }
        });
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
        const query = state.currentTopicId ? `?topic_id=${state.currentTopicId}` : "";
        const data = await fetchJson(`/api/papers/all${query}`);
        state.graphRows = data.papers || [];
        initAxisSelects(data.columns || []);
        updateGraph();
    }

    function updateGraph() {
        syncGraphControls();
        if (els.chartMode.value === "yearly_stacked") {
            return renderCategoricalGraph("yearly_stacked");
        }
        if (els.chartMode.value === "monthly_stacked") {
            return renderCategoricalGraph("monthly_stacked");
        }
        if (els.chartMode.value === "cumulative_line") {
            return renderCategoricalGraph("cumulative_line");
        }

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

        const xEngineering = ENGINEERING_KEYS.has(xKey);
        const yEngineering = ENGINEERING_KEYS.has(yKey);

        const xTicks = { color: "rgba(247,243,233,0.72)" };
        if (xEngineering) xTicks.callback = (v) => formatEngineering(v);

        const yTicks = { color: "rgba(247,243,233,0.72)" };
        if (yEngineering) yTicks.callback = (v) => formatEngineering(v);

        const ctx = document.getElementById("dbChart").getContext("2d");
        if (state.chart) {
            if (state.chart._midPanCleanup) state.chart._midPanCleanup();
            if (state.chart._dragZoomCleanup) state.chart._dragZoomCleanup();
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
                        ticks: xTicks,
                    },
                    y: {
                        type: yLog ? "logarithmic" : "linear",
                        title: { display: true, text: labelForKey(yKey), color: "#f7f3e9" },
                        grid: { color: "rgba(255,255,255,0.08)" },
                        ticks: yTicks,
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
                                const xFmt = xEngineering ? formatEngineering(context.raw.x) : context.raw.x;
                                const yFmt = yEngineering ? formatEngineering(context.raw.y) : context.raw.y;
                                return `${raw.category || "Other"} | ${raw.title || ""} | x=${xFmt}, y=${yFmt}`;
                            },
                        },
                    },
                    zoom: {
                        pan: {
                            enabled: true,
                            mode: "xy",
                            modifierKey: "ctrl",
                        },
                        zoom: {
                            wheel: { enabled: true },
                            pinch: { enabled: true },
                            mode: "xy",
                        },
                    },
                },
            },
        });
        attachMiddleClickPan(state.chart);
        attachDragZoom(state.chart, "xy");
    }

    function syncGraphControls() {
        const isScatter = els.chartMode.value === "scatter";
        els.scatterControls.classList.toggle("hidden", !isScatter);
        els.categoryFieldWrap.classList.toggle("hidden", isScatter);
    }

    function renderCategoricalGraph(mode) {
        const fieldKey = els.categoryField.value;
        const isMonthly = mode === "monthly_stacked";
        const grouped = isMonthly ? buildMonthCategorySeries(fieldKey) : buildYearCategorySeries(fieldKey);
        const periods = isMonthly ? grouped.months : grouped.years;
        const categories = grouped.categories;
        const countsByPeriod = isMonthly ? grouped.countsByMonth : grouped.countsByYear;
        const isStacked = mode === "yearly_stacked" || mode === "monthly_stacked";

        const datasets = categories.map((categoryName, idx) => {
            let runningTotal = 0;
            const values = periods.map((period) => {
                const count = countsByPeriod[period]?.[categoryName] || 0;
                if (mode === "cumulative_line") {
                    runningTotal += count;
                    return runningTotal;
                }
                return count;
            });

            return {
                label: categoryName,
                data: values,
                backgroundColor: colorForIndex(idx, mode === "cumulative_line" ? 0.18 : 0.72),
                borderColor: colorForIndex(idx, 0.92),
                borderWidth: mode === "cumulative_line" ? 2 : 1,
                fill: false,
                tension: 0.18,
            };
        });

        const ctx = document.getElementById("dbChart").getContext("2d");
        if (state.chart) {
            if (state.chart._midPanCleanup) state.chart._midPanCleanup();
            if (state.chart._dragZoomCleanup) state.chart._dragZoomCleanup();
            state.chart.destroy();
        }

        state.chart = new Chart(ctx, {
            type: mode === "cumulative_line" ? "line" : "bar",
            data: {
                labels: periods.map(String),
                datasets,
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                scales: {
                    x: {
                        stacked: isStacked,
                        title: { display: true, text: isMonthly ? "Month" : "Year", color: "#f7f3e9" },
                        grid: { color: "rgba(255,255,255,0.08)" },
                        ticks: {
                            color: "rgba(247,243,233,0.72)",
                            maxRotation: isMonthly ? 60 : 0,
                            minRotation: isMonthly ? 45 : 0,
                        },
                    },
                    y: {
                        stacked: isStacked,
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: mode === "cumulative_line" ? "Cumulative Count" : "Count",
                            color: "#f7f3e9",
                        },
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
                                return `${context.dataset.label}: ${context.parsed.y}`;
                            },
                        },
                    },
                    zoom: {
                        pan: { enabled: true, mode: "x" },
                        zoom: {
                            wheel: { enabled: true },
                            pinch: { enabled: true },
                            mode: "x",
                        },
                    },
                },
            },
        });
        attachMiddleClickPan(state.chart);
    }

    function buildYearCategorySeries(fieldKey) {
        const MAX_CATEGORIES = 9;
        const OTHERS_LABEL = "Others";
        const countsByYear = {};
        const categorySet = new Set();

        state.graphRows.forEach((paper) => {
            const yearValue = parseNumericValue(paper.year);
            const year = Number.isFinite(yearValue) ? Math.floor(yearValue) : NaN;
            const rawCategory = String(paper[fieldKey] || "").trim();
            if (!Number.isFinite(year) || !rawCategory) {
                return;
            }

            const categories = rawCategory.split(/\s*[,;/|]\s*/).map((item) => item.trim()).filter(Boolean);
            if (categories.length === 0) {
                return;
            }

            countsByYear[year] ||= {};
            categories.forEach((categoryName) => {
                categorySet.add(categoryName);
                countsByYear[year][categoryName] = (countsByYear[year][categoryName] || 0) + 1;
            });
        });

        const years = Object.keys(countsByYear).map(Number).sort((a, b) => a - b);
        const allCategories = Array.from(categorySet).sort((a, b) => {
            const totalA = years.reduce((sum, year) => sum + (countsByYear[year]?.[a] || 0), 0);
            const totalB = years.reduce((sum, year) => sum + (countsByYear[year]?.[b] || 0), 0);
            if (totalB !== totalA) {
                return totalB - totalA;
            }
            return a.localeCompare(b);
        });

        if (allCategories.length <= MAX_CATEGORIES) {
            return { years, categories: allCategories, countsByYear };
        }

        const topCategories = allCategories.slice(0, MAX_CATEGORIES);
        const tailCategories = new Set(allCategories.slice(MAX_CATEGORIES));

        years.forEach((year) => {
            let othersCount = 0;
            tailCategories.forEach((cat) => {
                othersCount += countsByYear[year]?.[cat] || 0;
            });
            if (othersCount > 0) {
                countsByYear[year][OTHERS_LABEL] = (countsByYear[year][OTHERS_LABEL] || 0) + othersCount;
            }
        });

        return { years, categories: [...topCategories, OTHERS_LABEL], countsByYear };
    }

    function buildMonthCategorySeries(fieldKey) {
        const MAX_CATEGORIES = 9;
        const OTHERS_LABEL = "Others";
        const countsByMonth = {};
        const categorySet = new Set();

        state.graphRows.forEach((paper) => {
            const month = String(paper.year_month || "").trim();
            if (!month || month.length < 7) return;
            const rawCategory = String(paper[fieldKey] || "").trim();
            if (!rawCategory) return;

            const cats = rawCategory.split(/\s*[,;/|]\s*/).map((s) => s.trim()).filter(Boolean);
            if (cats.length === 0) return;

            countsByMonth[month] ||= {};
            cats.forEach((cat) => {
                categorySet.add(cat);
                countsByMonth[month][cat] = (countsByMonth[month][cat] || 0) + 1;
            });
        });

        const months = Object.keys(countsByMonth).sort();
        const allCategories = Array.from(categorySet).sort((a, b) => {
            const totalA = months.reduce((sum, m) => sum + (countsByMonth[m]?.[a] || 0), 0);
            const totalB = months.reduce((sum, m) => sum + (countsByMonth[m]?.[b] || 0), 0);
            if (totalB !== totalA) return totalB - totalA;
            return a.localeCompare(b);
        });

        if (allCategories.length <= MAX_CATEGORIES) {
            return { months, categories: allCategories, countsByMonth };
        }

        const topCategories = allCategories.slice(0, MAX_CATEGORIES);
        const tailCategories = new Set(allCategories.slice(MAX_CATEGORIES));

        months.forEach((month) => {
            let othersCount = 0;
            tailCategories.forEach((cat) => {
                othersCount += countsByMonth[month]?.[cat] || 0;
            });
            if (othersCount > 0) {
                countsByMonth[month][OTHERS_LABEL] = (countsByMonth[month][OTHERS_LABEL] || 0) + othersCount;
            }
        });

        return { months, categories: [...topCategories, OTHERS_LABEL], countsByMonth };
    }

    function zoomScaleOut(chart, scaleId, ratio) {
        const scale = chart.scales[scaleId];
        if (!scale) return;
        if (scale.type === "logarithmic") {
            const lo = Math.log10(Math.max(scale.min, 1e-300));
            const hi = Math.log10(Math.max(scale.max, 1e-300));
            const center = (lo + hi) / 2;
            const newHalf = (hi - lo) / 2 / ratio;
            chart.zoomScale(scaleId, { min: Math.pow(10, center - newHalf), max: Math.pow(10, center + newHalf) }, "none");
        } else {
            const center = (scale.min + scale.max) / 2;
            const newHalf = (scale.max - scale.min) / 2 / ratio;
            chart.zoomScale(scaleId, { min: center - newHalf, max: center + newHalf }, "none");
        }
    }

    function attachDragZoom(chart, scaleMode) {
        const canvas = chart.canvas;
        const shell = canvas.closest(".chart-shell");

        const overlay = document.createElement("div");
        overlay.style.cssText = "position:absolute;inset:0;pointer-events:none;overflow:hidden;border-radius:22px;";
        const selRect = document.createElement("div");
        selRect.style.cssText = "position:absolute;display:none;pointer-events:none;box-sizing:border-box;border-width:1px;border-style:solid;";
        overlay.appendChild(selRect);
        shell.appendChild(overlay);

        let dragStart = null;
        let dragging = false;

        function canvasPos(e) {
            const r = canvas.getBoundingClientRect();
            return { x: e.clientX - r.left, y: e.clientY - r.top };
        }

        function toShellPos(pt) {
            const cr = canvas.getBoundingClientRect();
            const sr = shell.getBoundingClientRect();
            return { x: pt.x + (cr.left - sr.left), y: pt.y + (cr.top - sr.top) };
        }

        function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

        function onDown(e) {
            if (e.button !== 0 || e.ctrlKey) return;
            const pos = canvasPos(e);
            const ca = chart.chartArea;
            if (!ca || pos.x < ca.left || pos.x > ca.right || pos.y < ca.top || pos.y > ca.bottom) return;
            dragStart = pos;
            dragging = true;
            e.preventDefault();
        }

        function onMove(e) {
            if (!dragging || !dragStart) return;
            const pos = canvasPos(e);
            const ca = chart.chartArea;
            const sx = clamp(dragStart.x, ca.left, ca.right);
            const sy = clamp(dragStart.y, ca.top, ca.bottom);
            const ex = clamp(pos.x, ca.left, ca.right);
            const ey = clamp(pos.y, ca.top, ca.bottom);
            const w = Math.abs(ex - sx);
            const h = Math.abs(ey - sy);
            if (w < 5) { selRect.style.display = "none"; return; }
            const sp = toShellPos({ x: Math.min(sx, ex), y: Math.min(sy, ey) });
            const isRight = pos.x >= dragStart.x;
            selRect.style.display = "block";
            selRect.style.left = `${sp.x}px`;
            selRect.style.top = `${sp.y}px`;
            selRect.style.width = `${w}px`;
            selRect.style.height = `${scaleMode === "xy" ? h : ca.bottom - ca.top}px`;
            if (isRight) {
                selRect.style.borderColor = "rgba(21,184,166,0.8)";
                selRect.style.background = "rgba(21,184,166,0.1)";
            } else {
                selRect.style.borderColor = "rgba(255,182,72,0.8)";
                selRect.style.background = "rgba(255,182,72,0.08)";
            }
        }

        function onUp(e) {
            if (!dragging || !dragStart || e.button !== 0) return;
            dragging = false;
            selRect.style.display = "none";

            const pos = canvasPos(e);
            const ca = chart.chartArea;
            const dx = pos.x - dragStart.x;
            if (Math.abs(dx) < 10) { dragStart = null; return; }

            const isZoomIn = dx > 0;
            const caW = ca.right - ca.left;
            const caH = ca.bottom - ca.top;
            const sx = clamp(dragStart.x, ca.left, ca.right);
            const sy = clamp(dragStart.y, ca.top, ca.bottom);
            const ex = clamp(pos.x, ca.left, ca.right);
            const ey = clamp(pos.y, ca.top, ca.bottom);
            const selW = Math.abs(ex - sx);
            const selH = Math.abs(ey - sy);
            const ratioX = Math.max(selW / caW, 0.02);
            const ratioY = Math.max(selH / caH, 0.02);

            if (isZoomIn) {
                const xMin = chart.scales.x.getValueForPixel(Math.min(sx, ex));
                const xMax = chart.scales.x.getValueForPixel(Math.max(sx, ex));
                chart.zoomScale("x", { min: xMin, max: xMax }, "none");
                if (scaleMode === "xy" && selH > 5) {
                    const yMin = chart.scales.y.getValueForPixel(Math.max(sy, ey));
                    const yMax = chart.scales.y.getValueForPixel(Math.min(sy, ey));
                    chart.zoomScale("y", { min: yMin, max: yMax }, "none");
                }
            } else {
                zoomScaleOut(chart, "x", ratioX);
                if (scaleMode === "xy") zoomScaleOut(chart, "y", ratioY);
            }

            chart.update("none");
            dragStart = null;
        }

        canvas.addEventListener("mousedown", onDown);
        window.addEventListener("mousemove", onMove);
        window.addEventListener("mouseup", onUp);

        chart._dragZoomCleanup = () => {
            canvas.removeEventListener("mousedown", onDown);
            window.removeEventListener("mousemove", onMove);
            window.removeEventListener("mouseup", onUp);
            if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
        };
    }

    function attachMiddleClickPan(chart) {
        const canvas = chart.canvas;
        let active = false;
        let lastX = 0;
        let lastY = 0;

        function onDown(e) {
            if (e.button !== 1) return;
            e.preventDefault();
            active = true;
            lastX = e.clientX;
            lastY = e.clientY;
        }

        function onMove(e) {
            if (!active) return;
            const dx = e.clientX - lastX;
            const dy = e.clientY - lastY;
            lastX = e.clientX;
            lastY = e.clientY;
            chart.pan({ x: dx, y: dy }, undefined, "none");
        }

        function onUp(e) {
            if (e.button === 1) active = false;
        }

        canvas.addEventListener("mousedown", onDown);
        window.addEventListener("mousemove", onMove);
        window.addEventListener("mouseup", onUp);

        chart._midPanCleanup = () => {
            canvas.removeEventListener("mousedown", onDown);
            window.removeEventListener("mousemove", onMove);
            window.removeEventListener("mouseup", onUp);
        };
    }

    function colorForIndex(index, alpha) {
        const palette = [
            [21, 184, 166],
            [255, 182, 72],
            [120, 177, 255],
            [255, 111, 97],
            [162, 123, 255],
            [109, 213, 130],
            [255, 140, 205],
            [92, 218, 255],
            [217, 196, 85],
            [255, 154, 96],
        ];
        const [r, g, b] = palette[index % palette.length];
        return `rgba(${r}, ${g}, ${b}, ${alpha})`;
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

    const ENGINEERING_KEYS = new Set([
        "speed_seconds", "endurance_cycles", "memory_window_voltage", "memory_window_ratio",
    ]);

    function formatEngineering(value) {
        if (!Number.isFinite(value)) return "";
        if (value === 0) return "0";
        const abs = Math.abs(value);
        const tiers = [
            [1e12, "T"], [1e9, "G"], [1e6, "M"], [1e3, "k"],
            [1, ""], [1e-3, "m"], [1e-6, "µ"], [1e-9, "n"], [1e-12, "p"], [1e-15, "f"],
        ];
        for (const [base, sym] of tiers) {
            if (abs >= base || base === 1e-15) {
                const scaled = value / base;
                const absScaled = Math.abs(scaled);
                const str = absScaled >= 100
                    ? scaled.toFixed(0)
                    : absScaled >= 10
                        ? scaled.toFixed(1)
                        : scaled.toPrecision(3);
                return `${parseFloat(str)}${sym}`;
            }
        }
        return String(value);
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
