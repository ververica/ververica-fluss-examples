// Courier tracking and update functionality

let courier = {}; // Will be initialized from the page
const connectionStatus = document.getElementById('connection-status');

function updateCourierInfo(updatedCourier) {
    // Update all courier information in the UI
    document.querySelector('.courier-info .info-row:nth-child(2) div:nth-child(2)').textContent = updatedCourier.courierId;
    document.querySelector('.courier-info .info-row:nth-child(3) div:nth-child(2)').textContent = updatedCourier.orderId;

    const locationElement = document.querySelector('.courier-info .info-row:nth-child(4) div:nth-child(2)');
    locationElement.innerHTML = `Latitude: <span>${updatedCourier.courierLatitude}</span>, Longitude: <span>${updatedCourier.courierLongitude}</span>`;

    document.querySelector('.courier-info .info-row:nth-child(5) div:nth-child(2)').textContent = updatedCourier.speedKph + ' km/h';

    const statusElement = document.querySelector('.courier-info .info-row:nth-child(6) div:nth-child(2)');
    statusElement.textContent = updatedCourier.isAvailable ? 'Available' : 'Unavailable';
    statusElement.className = updatedCourier.isAvailable ? 'available' : 'unavailable';

    document.querySelector('.courier-info .info-row:nth-child(7) div:nth-child(2)').textContent = 
        new Date(updatedCourier.lastUpdatedTimestamp).toLocaleString();

    // Update map
    updateMap(updatedCourier);
}

function updateMap(updatedCourier) {
    const mapContainer = document.getElementById('map');
    mapContainer.innerHTML = `
        <div style="background-color: #3aa89f; height: 100%; display: flex; align-items: center; justify-content: center; border-radius: 8px; color: #0a192f; font-weight: bold;">
            <p>Map showing courier location at coordinates: ${updatedCourier.courierLatitude}, ${updatedCourier.courierLongitude}</p>
        </div>
    `;
}

function connectToSSE() {
    connectionStatus.textContent = 'Connecting...';
    connectionStatus.className = '';

    // Create a new EventSource connection to the SSE endpoint
    const eventSource = new EventSource(`/api/courier/${courier.courierId}/stream`);

    eventSource.onopen = () => {
        connectionStatus.textContent = 'Connected';
        connectionStatus.className = 'connected';
    };

    eventSource.addEventListener('courier-update', (event) => {
        try {
            const updatedCourier = JSON.parse(event.data);
            courier = updatedCourier; // Update the courier object
            updateCourierInfo(updatedCourier);
        } catch (error) {
            console.error('Error processing courier data:', error);
        }
    });

    eventSource.onerror = () => {
        connectionStatus.textContent = 'Disconnected - Reconnecting...';
        connectionStatus.className = 'disconnected';

        eventSource.close();
        setTimeout(connectToSSE, 5000);
    };
}

// Initialize when the page loads
document.addEventListener('DOMContentLoaded', function() {
    // Get courier data from the page
    courier = courierData;

    // Initial map setup
    updateMap(courier);

    // Connect to SSE for real-time updates
    connectToSSE();
});
