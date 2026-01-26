/**
 * Fabric Data Quality Framework - Interactive Documentation
 * JavaScript functionality for tabs, theme toggle, and copy buttons
 */

// ============================================
// Theme Toggle
// ============================================
function toggleTheme() {
    const body = document.body;
    const button = document.querySelector('.theme-toggle');

    if (body.getAttribute('data-theme') === 'light') {
        body.removeAttribute('data-theme');
        button.textContent = '🌙';
        localStorage.setItem('theme', 'dark');
    } else {
        body.setAttribute('data-theme', 'light');
        button.textContent = '☀️';
        localStorage.setItem('theme', 'light');
    }
}

// Load saved theme
document.addEventListener('DOMContentLoaded', () => {
    const savedTheme = localStorage.getItem('theme');
    const button = document.querySelector('.theme-toggle');

    if (savedTheme === 'light') {
        document.body.setAttribute('data-theme', 'light');
        button.textContent = '☀️';
    }

    // Initialize active nav link
    updateActiveNavLink();
});

// ============================================
// Navigation
// ============================================
function updateActiveNavLink() {
    const sections = document.querySelectorAll('section[id]');
    const navLinks = document.querySelectorAll('.nav-link');

    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                navLinks.forEach(link => {
                    link.classList.remove('active');
                    if (link.getAttribute('href') === `#${entry.target.id}`) {
                        link.classList.add('active');
                    }
                });
            }
        });
    }, {
        rootMargin: '-50% 0px -50% 0px'
    });

    sections.forEach(section => observer.observe(section));
}

// Smooth scroll for nav links
document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', (e) => {
        e.preventDefault();
        const targetId = link.getAttribute('href');
        const targetSection = document.querySelector(targetId);
        if (targetSection) {
            targetSection.scrollIntoView({ behavior: 'smooth' });
        }
    });
});

// ============================================
// Platform Tabs
// ============================================
function showPlatform(platform) {
    // Update tab buttons
    document.querySelectorAll('.platform-tab').forEach(tab => {
        tab.classList.remove('active');
    });
    event.target.classList.add('active');

    // Update panels
    document.querySelectorAll('.platform-panel').forEach(panel => {
        panel.classList.remove('active');
    });
    document.getElementById(`platform-${platform}`).classList.add('active');
}

// ============================================
// Example Tabs
// ============================================
function showExample(example) {
    // Update tab buttons
    document.querySelectorAll('.example-tab').forEach(tab => {
        tab.classList.remove('active');
    });
    event.target.classList.add('active');

    // Update panels
    document.querySelectorAll('.example-panel').forEach(panel => {
        panel.classList.remove('active');
    });
    document.getElementById(`example-${example}`).classList.add('active');
}

// ============================================
// Setup Tabs (Quick Start)
// ============================================
function showSetup(setup) {
    // Update tab buttons
    document.querySelectorAll('.setup-tab').forEach(tab => {
        tab.classList.remove('active');
    });
    event.target.classList.add('active');

    // Update panels
    document.querySelectorAll('.setup-panel').forEach(panel => {
        panel.classList.remove('active');
    });
    document.getElementById(`setup-${setup}`).classList.add('active');
}

// ============================================
// Copy to Clipboard
// ============================================
function copyCode(button) {
    const codeBlock = button.closest('.code-block');
    const code = codeBlock.querySelector('code').textContent;

    navigator.clipboard.writeText(code).then(() => {
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        button.style.background = '#10b981';
        button.style.borderColor = '#10b981';
        button.style.color = 'white';

        setTimeout(() => {
            button.textContent = originalText;
            button.style.background = '';
            button.style.borderColor = '';
            button.style.color = '';
        }, 2000);
    }).catch(err => {
        console.error('Failed to copy:', err);
        button.textContent = 'Failed';
        setTimeout(() => {
            button.textContent = 'Copy';
        }, 2000);
    });
}

// ============================================
// Smooth scroll for hero buttons
// ============================================
document.querySelectorAll('.hero-actions .btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
        const href = btn.getAttribute('href');
        if (href && href.startsWith('#')) {
            e.preventDefault();
            const target = document.querySelector(href);
            if (target) {
                target.scrollIntoView({ behavior: 'smooth' });
            }
        }
    });
});

// ============================================
// Add animation on scroll
// ============================================
const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
};

const fadeInObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            entry.target.style.opacity = '1';
            entry.target.style.transform = 'translateY(0)';
        }
    });
}, observerOptions);

// Apply to feature cards and steps
document.querySelectorAll('.feature-card, .step, .api-card, .workflow-step').forEach(el => {
    el.style.opacity = '0';
    el.style.transform = 'translateY(20px)';
    el.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
    fadeInObserver.observe(el);
});

// ============================================
// Architecture Diagram Interactivity
// ============================================
const archDetails = {
    source: {
        title: 'Data Sources',
        description: 'The framework supports the following data formats and sources:',
        items: [
            'CSV, Parquet, JSON, and Excel files',
            'Delta Lake tables',
            'Spark DataFrames (direct validation)',
            'Pandas DataFrames',
            'ABFSS paths in Azure Fabric'
        ]
    },
    bronze: {
        title: 'Bronze Layer Validation',
        description: 'Raw data ingestion layer with essential data quality checks:',
        items: [
            '<strong>Objective:</strong> Verify successful data ingestion',
            '<strong>Checks:</strong> Table not empty, required columns exist, basic schema validation',
            '<strong>Null tolerance:</strong> Higher threshold (10-20%)',
            '<strong>On failure:</strong> Log warning, continue processing',
            '<strong>Use case:</strong> Detect ingestion failures early'
        ]
    },
    silver: {
        title: 'Silver Layer Validation',
        description: 'Cleaned and conformed data layer with business rule validation:',
        items: [
            '<strong>Objective:</strong> Ensure data quality for analytics',
            '<strong>Checks:</strong> Uniqueness, valid ranges, referential integrity',
            '<strong>Null tolerance:</strong> Medium threshold (5%)',
            '<strong>On failure:</strong> Send alert, quarantine affected records',
            '<strong>Use case:</strong> Business logic validation'
        ]
    },
    gold: {
        title: 'Gold Layer Validation',
        description: 'Business-ready data layer with zero tolerance for errors:',
        items: [
            '<strong>Objective:</strong> Protect reports and analytics from data issues',
            '<strong>Checks:</strong> All previous checks plus aggregation accuracy',
            '<strong>Null tolerance:</strong> Zero for dimension columns',
            '<strong>On failure:</strong> Halt pipeline, prevent data propagation',
            '<strong>Use case:</strong> Final quality gate before BI consumption'
        ]
    },
    analytics: {
        title: 'Analytics and Reporting',
        description: 'Validated data is made available to downstream consumers:',
        items: [
            'Power BI dashboards with trusted data',
            'Machine learning models',
            'Data science notebooks',
            'SQL analytics queries',
            'API consumers'
        ]
    }
};

function showArchDetail(component) {
    const panel = document.getElementById('arch-detail');
    const detail = archDetails[component];

    if (!detail) return;

    panel.innerHTML = `
        <div class="arch-detail-content">
            <h4>${detail.title}</h4>
            <p>${detail.description}</p>
            <ul>
                ${detail.items.map(item => `<li>${item}</li>`).join('')}
            </ul>
        </div>
    `;

    // Add highlight animation
    panel.style.borderColor = 'var(--accent-primary)';
    setTimeout(() => {
        panel.style.borderColor = 'var(--border-color)';
    }, 500);
}

console.log('📊 DQ Framework Documentation loaded');
