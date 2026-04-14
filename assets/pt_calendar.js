// Traduz o calendário do dcc.DatePickerSingle para português via MutationObserver
(function () {
    var MONTHS = {
        January: "Janeiro", February: "Fevereiro", March: "Março",
        April: "Abril", May: "Maio", June: "Junho",
        July: "Julho", August: "Agosto", September: "Setembro",
        October: "Outubro", November: "Novembro", December: "Dezembro"
    };
    var DAYS = {
        Mo: "Seg", Tu: "Ter", We: "Qua",
        Th: "Qui", Fr: "Sex", Sa: "Sáb", Su: "Dom"
    };

    function translateNode(node) {
        if (node.nodeType === Node.TEXT_NODE) {
            var t = node.textContent.trim();
            if (MONTHS[t]) node.textContent = node.textContent.replace(t, MONTHS[t]);
            else if (DAYS[t])  node.textContent = DAYS[t];
        } else if (node.nodeType === Node.ELEMENT_NODE) {
            node.childNodes.forEach(translateNode);
        }
    }

    function translateAll() {
        document.querySelectorAll(
            ".CalendarMonth_caption, .DayPicker_weekHeader_li, " +
            ".CalendarMonthGrid_month__horizontal, .CalendarMonth"
        ).forEach(translateNode);
    }

    var observer = new MutationObserver(function (mutations) {
        mutations.forEach(function (m) {
            m.addedNodes.forEach(function (n) {
                if (n.nodeType === Node.ELEMENT_NODE) translateNode(n);
            });
        });
        translateAll();
    });

    observer.observe(document.body, { childList: true, subtree: true });

    // Garante tradução após carregamento completo
    document.addEventListener("DOMContentLoaded", translateAll);
    setTimeout(translateAll, 500);
})();
