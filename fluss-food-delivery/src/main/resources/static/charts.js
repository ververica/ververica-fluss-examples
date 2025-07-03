document.addEventListener('DOMContentLoaded', () => {
    // DOM elements
    const connectionStatus = document.getElementById('connection-status');
    const recordCount = document.getElementById('record-count');
    const barChartCanvas = document.getElementById('bar-chart');
    const lineChartCanvas = document.getElementById('line-chart');

    // Track the number of records received
    let recordsReceived = 0;

    // Data structures for charts
    const paymentMethods = ['Credit Card', 'PayPal', 'Revolut', 'Apple Pay'];
    const colors = ['#0a2342', '#1d3557', '#4d7ea8', '#a3c1e6'];
    
    // Data for bar chart - total amount per payment method
    const totalAmountData = {
        'Credit Card': 0,
        'PayPal': 0,
        'Revolut': 0,
        'Apple Pay': 0
    };

    // Data for line chart - average price per payment method
    const averagePriceData = {
        labels: [],
        datasets: paymentMethods.map((method, index) => ({
            label: method,
            data: [],
            borderColor: colors[index],
            backgroundColor: colors[index] + '20',
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
                label: 'Total Amount ($)',
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
                            return '$' + value;
                        }
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
                            return '$' + context.raw.toFixed(2);
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
                            return '$' + value;
                        }
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.dataset.label + ': $' + context.raw.toFixed(2);
                        }
                    }
                }
            }
        }
    });

    // Function to update the bar chart
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

    // Function to calculate and update the line chart every 10 seconds
    const updateLineChart = () => {
        const now = new Date();
        const timeLabel = now.toLocaleTimeString();
        
        // Add new time label
        averagePriceData.labels.push(timeLabel);
        
        // Calculate averages for each payment method
        paymentMethods.forEach((method, index) => {
            const methodData = intervalData[method];
            const average = methodData.count > 0 ? methodData.total / methodData.count : 0;
            
            // Add the average to the dataset
            averagePriceData.datasets[index].data.push(average);
            
            // Reset the interval data
            methodData.count = 0;
            methodData.total = 0;
        });
        
        // Keep only the last 10 data points to avoid cluttering
        if (averagePriceData.labels.length > 10) {
            averagePriceData.labels.shift();
            averagePriceData.datasets.forEach(dataset => {
                dataset.data.shift();
            });
        }
        
        // Update the chart
        lineChart.update();
        
        // Update the timestamp
        intervalData.timestamp = now;
    };

    // Set up interval for updating the line chart (every 10 seconds)
    setInterval(updateLineChart, 10000);

    // Function to connect to the SSE endpoint
    const connectToSSE = () => {
        connectionStatus.textContent = 'Connecting...';
        connectionStatus.className = '';

        // Create a new EventSource connection to the SSE endpoint
        const eventSource = new EventSource('http://localhost:8080/api/payments');

        // Handle connection open
        eventSource.onopen = () => {
            connectionStatus.textContent = 'Connected';
            connectionStatus.className = 'connected';
        };

        // Handle incoming messages
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

        // Handle errors
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