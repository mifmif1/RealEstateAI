// Format tooltip values as integers (no decimals)
window.dccFunctions = window.dccFunctions || {};
window.dccFunctions.intFormatter = function(value) {
    return Math.round(value).toString();
}
