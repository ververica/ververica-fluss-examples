document.addEventListener('DOMContentLoaded', () => {
    // DOM elements
    const connectionStatus = document.getElementById('connection-status');
    const recordCount = document.getElementById('record-count');
    const barChartCanvas = document.getElementById('bar-chart');
    const lineChartCanvas = document.getElementById('line-chart');

    let recordsReceived = 0;

    const paymentMethods = ['Credit Card', 'PayPal', 'Revolut', 'Apple Pay'];
    const colors = ['#8b0000', '#b8860b', '#2980b9', '#9c27b0']; // darker-red, dark-yellow, blue, bright-purple

    // Data for bar chart - total amount per payment method
    const totalAmountData = {
        'Credit Card': 0,
        'PayPal': 0,
        'Revolut': 0,
        'Apple Pay': 0
    };

    const averagePriceData = {
        labels: [],
        datasets: paymentMethods.map((method, index) => ({
            label: method,
            data: [],
            borderColor: colors[index],
            backgroundColor: colors[index] + '40',
            tension: 0.3,
            fill: false
        }))
    };

    // Tracking data for calculating averages
    const intervalData = {
        'Credit Card': { count: 0, total: 0 },
        'PayPal': { count: 0, total: 0 },
        'Revolut': { count: 0, total: 0 },
        'Apple Pay': { count: 0, total: 0 },
        timestamp: new Date()
    };

    // Initialize bar chart
    const barChart = new Chart(barChartCanvas, {
        type: 'bar',
        data: {
            labels: paymentMethods,
            datasets: [{
                label: 'Total Amount (€)',
                data: paymentMethods.map(method => totalAmountData[method]),
                backgroundColor: colors,
                borderColor: colors.map(color => color + '80'),
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return '€' + value;
                        },
                        color: '#e6f1ff'
                    }
                },
                x: {
                    ticks: {
                        color: '#e6f1ff'
                    }
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return '€' + context.raw.toFixed(2);
                        }
                    }
                }
            }
        }
    });

    // Initialize line chart
    const lineChart = new Chart(lineChartCanvas, {
        type: 'line',
        data: averagePriceData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return '€' + value;
                        },
                        color: '#e6f1ff'
                    }
                },
                x: {
                    ticks: {
                        color: '#e6f1ff'
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.dataset.label + ': €' + context.raw.toFixed(2);
                        }
                    }
                }
            }
        }
    });

    const updateBarChart = () => {
        barChart.data.datasets[0].data = paymentMethods.map(method => totalAmountData[method]);
        barChart.update();
    };

    // Function to update interval data and calculate averages
    const updateIntervalData = (payment) => {
        if (intervalData[payment.paymentMethod]) {
            intervalData[payment.paymentMethod].count++;
            intervalData[payment.paymentMethod].total += payment.totalAmount;
        }
    };

    const updateLineChart = () => {
        const now = new Date();
        const timeLabel = now.toLocaleTimeString();

        averagePriceData.labels.push(timeLabel);

        paymentMethods.forEach((method, index) => {
            const methodData = intervalData[method];
            const average = methodData.count > 0 ? methodData.total / methodData.count : 0;

            averagePriceData.datasets[index].data.push(average);

            methodData.count = 0;
            methodData.total = 0;
        });

        if (averagePriceData.labels.length > 60) {
            averagePriceData.labels.shift();
            averagePriceData.datasets.forEach(dataset => {
                dataset.data.shift();
            });
        }

        lineChart.update();

        intervalData.timestamp = now;
    };

    setInterval(updateLineChart, 1000);

    const connectToSSE = () => {
        connectionStatus.textContent = 'Connecting...';
        connectionStatus.className = '';

        const eventSource = new EventSource('http://localhost:8080/api/payments');

        eventSource.onopen = () => {
            connectionStatus.textContent = 'Connected';
            connectionStatus.className = 'connected';
        };

        eventSource.onmessage = (event) => {
            try {
                const payment = JSON.parse(event.data);

                // Update total amount data
                if (totalAmountData[payment.paymentMethod] !== undefined) {
                    totalAmountData[payment.paymentMethod] += payment.totalAmount;
                    updateBarChart();
                }

                // Update interval data for line chart
                updateIntervalData(payment);

                // Update record count
                recordsReceived++;
                recordCount.textContent = `Records: ${recordsReceived}`;
            } catch (error) {
                console.error('Error processing payment data:', error);
            }
        };

        eventSource.onerror = () => {
            connectionStatus.textContent = 'Disconnected - Reconnecting...';
            connectionStatus.className = 'disconnected';

            // Close the current connection
            eventSource.close();

            // Try to reconnect after a delay
            setTimeout(connectToSSE, 5000);
        };
    };

    // Start the SSE connection
    connectToSSE();
});
