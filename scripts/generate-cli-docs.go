//go:build tools
// +build tools

package main

import (
	"bufio"
	"fmt"
	"html"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

func main() {
	fmt.Println("Generating CLI documentation...")

	fmt.Println("Updating Go dependencies...")
	modCmd := exec.Command("go", "mod", "tidy")
	modCmd.Stdout = os.Stdout
	modCmd.Stderr = os.Stderr
	if err := modCmd.Run(); err != nil {
		fmt.Printf("Error updating dependencies: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll("bin", 0755); err != nil {
		fmt.Printf("Error creating bin directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Building CLI...")
	buildCmd := exec.Command("go", "build", "-o", "bin/mirror-cli", "./cmd/mirror-cli")
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		fmt.Printf("Error building CLI: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Generating single consolidated Markdown documentation file...")
	docsCmd := exec.Command("./bin/mirror-cli", "docs", "generate", "--file", "./docs/cli-commands.md")
	docsCmd.Stdout = os.Stdout
	docsCmd.Stderr = os.Stderr

	if err := docsCmd.Run(); err != nil {
		fmt.Printf("Error generating docs: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Converting Markdown documentation to HTML...")
	if err := convertMarkdownToHTML("./docs/cli-commands.md", "./docs/cli-commands.html"); err != nil {
		fmt.Printf("Error converting to HTML: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("CLI documentation generated successfully!")
	fmt.Println("- Markdown file: ./docs/cli-commands.md")
	fmt.Println("- HTML file: ./docs/cli-commands.html")
}

// convertMarkdownToHTML converts the comprehensive Markdown file to HTML
func convertMarkdownToHTML(markdownFile, htmlFile string) error {
	file, err := os.Open(markdownFile)
	if err != nil {
		return fmt.Errorf("failed to open markdown file: %v", err)
	}
	defer file.Close()

	htmlOutput, err := os.Create(htmlFile)
	if err != nil {
		return fmt.Errorf("failed to create HTML file: %v", err)
	}
	defer htmlOutput.Close()

	htmlOutput.WriteString(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CLI Command Reference - kaf-mirror</title>
    <style>
        * {
            box-sizing: border-box;
        }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif; 
            line-height: 1.5;
            margin: 0;
            padding: 0;
            color: #2d3748;
            background-color: #f7fafc;
        }
        .container {
            display: flex;
            min-height: 100vh;
        }
        .sidebar {
            width: 280px;
            background: #ffffff;
            border-right: 1px solid #e2e8f0;
            padding: 20px;
            position: fixed;
            height: 100vh;
            overflow-y: auto;
            box-shadow: 2px 0 4px rgba(0,0,0,0.1);
        }
        .sidebar h2 {
            font-size: 1.25rem;
            font-weight: 600;
            color: #2d3748;
            margin: 0 0 20px 0;
            padding-bottom: 10px;
            border-bottom: 2px solid #4299e1;
        }
        .sidebar ul {
            list-style: none;
            padding: 0;
            margin: 0;
        }
        .sidebar li {
            margin: 0;
        }
        .sidebar a {
            display: block;
            padding: 8px 12px;
            color: #4a5568;
            text-decoration: none;
            border-radius: 6px;
            font-size: 0.9rem;
            transition: all 0.2s ease;
        }
        .sidebar a:hover {
            background-color: #edf2f7;
            color: #2b6cb0;
        }
        .sidebar .level-1 {
            font-weight: 500;
            padding-left: 12px;
        }
        .sidebar .level-2 {
            padding-left: 24px;
            font-size: 0.85rem;
        }
        .sidebar .level-3 {
            padding-left: 36px;
            font-size: 0.8rem;
            color: #718096;
        }
        .content {
            flex: 1;
            margin-left: 280px;
            padding: 40px 40px 40px 60px;
            max-width: calc(100% - 280px);
            background: white;
        }
        .content h1 {
            font-size: 2.5rem;
            font-weight: 700;
            color: #1a202c;
            margin: 0 0 20px 0;
            padding-bottom: 15px;
            border-bottom: 3px solid #4299e1;
        }
        .content h2 {
            font-size: 1.75rem;
            font-weight: 600;
            color: #2d3748;
            margin: 40px 0 15px 0;
            padding-left: 15px;
            border-left: 4px solid #4299e1;
            scroll-margin-top: 20px;
        }
        .content h3 {
            font-size: 1.25rem;
            font-weight: 500;
            color: #4a5568;
            margin: 25px 0 10px 0;
            scroll-margin-top: 20px;
        }
        .content h4 {
            font-size: 1.1rem;
            font-weight: 500;
            color: #718096;
            margin: 20px 0 8px 0;
            scroll-margin-top: 20px;
        }
        .content h5, .content h6 {
            font-size: 1rem;
            font-weight: 500;
            color: #a0aec0;
            margin: 15px 0 8px 0;
            scroll-margin-top: 20px;
        }
        .content p {
            margin: 8px 0;
            color: #4a5568;
            line-height: 1.6;
        }
        .content strong {
            color: #2d3748;
            font-weight: 600;
        }
        .content code {
            background-color: #edf2f7;
            color: #d53f8c;
            padding: 2px 6px;
            border-radius: 4px;
            font-family: 'SF Mono', 'Monaco', 'Menlo', 'Consolas', 'Liberation Mono', monospace;
            font-size: 0.85em;
        }
        .content pre {
            background-color: #2d3748;
            color: #e2e8f0;
            padding: 20px;
            border-radius: 8px;
            overflow-x: auto;
            font-family: 'SF Mono', 'Monaco', 'Menlo', 'Consolas', 'Liberation Mono', monospace;
            font-size: 0.9em;
            margin: 15px 0;
            border: 1px solid #4a5568;
        }
        .content pre code {
            background: none;
            color: inherit;
            padding: 0;
        }
        .content ul {
            padding-left: 20px;
            margin: 10px 0;
        }
        .content li {
            margin: 4px 0;
            color: #4a5568;
        }
        .content hr {
            border: none;
            height: 1px;
            background: linear-gradient(to right, #e2e8f0, #cbd5e0, #e2e8f0);
            margin: 40px 0;
        }
        .section-divider {
            margin: 30px 0;
            padding: 15px 0;
            border-bottom: 1px solid #e2e8f0;
        }
        html {
            scroll-behavior: smooth;
        }
        @media (max-width: 768px) {
            .sidebar {
                display: none;
            }
            .content {
                margin-left: 0;
                max-width: 100%;
                padding: 20px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <nav class="sidebar">
            <h2>Commands</h2>
            <ul id="nav-list">
            </ul>
        </nav>
        <main class="content">
`)

	// First pass: collect headers for navigation
	var navItems []NavItem
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if level := getHeaderLevel(line); level > 0 {
			title := strings.TrimSpace(line[level:])
			anchor := generateAnchor(title)
			navItems = append(navItems, NavItem{
				Title:  title,
				Anchor: anchor,
				Level:  level,
			})
		}
	}
	file.Close()

	htmlOutput.WriteString(generateNavigation(navItems))

	// Second pass: generate content
	file, err = os.Open(markdownFile)
	if err != nil {
		return fmt.Errorf("failed to reopen markdown file: %v", err)
	}
	defer file.Close()

	scanner = bufio.NewScanner(file)
	inCodeBlock := false

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "```") {
			if inCodeBlock {
				htmlOutput.WriteString("</pre>\n")
				inCodeBlock = false
			} else {
				htmlOutput.WriteString("<pre><code>")
				inCodeBlock = true
			}
			continue
		}

		if inCodeBlock {
			htmlOutput.WriteString(html.EscapeString(line) + "\n")
			continue
		}

		htmlLine := convertMarkdownLine(line)
		htmlOutput.WriteString(htmlLine + "\n")
	}

	if inCodeBlock {
		htmlOutput.WriteString("</code></pre>\n")
	}

	htmlOutput.WriteString(`
        </main>
    </div>
    <script>
        // Smooth scrolling for anchor links
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {
            anchor.addEventListener('click', function (e) {
                e.preventDefault();
                const target = document.querySelector(this.getAttribute('href'));
                if (target) {
                    target.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                }
            });
        });
        
        // Highlight current section in navigation
        const observer = new IntersectionObserver((entries) => {
            entries.forEach((entry) => {
                if (entry.isIntersecting) {
                    const id = entry.target.getAttribute('id');
                    if (id) {
                        document.querySelectorAll('.sidebar a').forEach(link => {
                            link.style.backgroundColor = '';
                            link.style.color = '#4a5568';
                        });
                        const activeLink = document.querySelector('.sidebar a[href="#' + id + '"]');
                        if (activeLink) {
                            activeLink.style.backgroundColor = '#bee3f8';
                            activeLink.style.color = '#2b6cb0';
                        }
                    }
                }
            });
        }, { rootMargin: '-20% 0px -80% 0px' });
        
        document.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach(header => {
            observer.observe(header);
        });
    </script>
</body>
</html>
`)

	return scanner.Err()
}

// NavItem represents a navigation item
type NavItem struct {
	Title  string
	Anchor string
	Level  int
}

// convertMarkdownLine converts a single line of markdown to HTML
func convertMarkdownLine(line string) string {
	if line == "" {
		return "<br>"
	}

	// Headers with anchors
	if strings.HasPrefix(line, "######") {
		title := strings.TrimSpace(line[6:])
		anchor := generateAnchor(title)
		return fmt.Sprintf(`<h6 id="%s">%s</h6>`, anchor, html.EscapeString(title))
	} else if strings.HasPrefix(line, "#####") {
		title := strings.TrimSpace(line[5:])
		anchor := generateAnchor(title)
		return fmt.Sprintf(`<h5 id="%s">%s</h5>`, anchor, html.EscapeString(title))
	} else if strings.HasPrefix(line, "####") {
		title := strings.TrimSpace(line[4:])
		anchor := generateAnchor(title)
		return fmt.Sprintf(`<h4 id="%s">%s</h4>`, anchor, html.EscapeString(title))
	} else if strings.HasPrefix(line, "###") {
		title := strings.TrimSpace(line[3:])
		anchor := generateAnchor(title)
		return fmt.Sprintf(`<h3 id="%s">%s</h3>`, anchor, html.EscapeString(title))
	} else if strings.HasPrefix(line, "##") {
		title := strings.TrimSpace(line[2:])
		anchor := generateAnchor(title)
		return fmt.Sprintf(`<h2 id="%s">%s</h2>`, anchor, html.EscapeString(title))
	} else if strings.HasPrefix(line, "#") {
		title := strings.TrimSpace(line[1:])
		anchor := generateAnchor(title)
		return fmt.Sprintf(`<h1 id="%s">%s</h1>`, anchor, html.EscapeString(title))
	}

	// Horizontal rule
	if strings.TrimSpace(line) == "---" {
		return "<hr>"
	}

	// Lists
	if strings.HasPrefix(line, "- ") {
		return "<li>" + processInlineMarkdown(html.EscapeString(strings.TrimSpace(line[2:]))) + "</li>"
	}

	// Regular paragraph
	return "<p>" + processInlineMarkdown(html.EscapeString(line)) + "</p>"
}

// processInlineMarkdown processes inline markdown like bold, code, etc.
func processInlineMarkdown(text string) string {
	// Bold text
	boldRe := regexp.MustCompile(`\*\*([^*]+)\*\*`)
	text = boldRe.ReplaceAllString(text, "<strong>$1</strong>")

	// Inline code - be careful not to double-escape
	codeRe := regexp.MustCompile("`([^`]+)`")
	text = codeRe.ReplaceAllString(text, "<code>$1</code>")

	return text
}

// getHeaderLevel returns the level of a markdown header (1-6), or 0 if not a header
func getHeaderLevel(line string) int {
	if strings.HasPrefix(line, "######") {
		return 6
	} else if strings.HasPrefix(line, "#####") {
		return 5
	} else if strings.HasPrefix(line, "####") {
		return 4
	} else if strings.HasPrefix(line, "###") {
		return 3
	} else if strings.HasPrefix(line, "##") {
		return 2
	} else if strings.HasPrefix(line, "#") {
		return 1
	}
	return 0
}

// generateAnchor generates a URL-safe anchor from a title
func generateAnchor(title string) string {
	anchor := strings.ToLower(title)

	// Replace spaces and special characters with hyphens
	reg := regexp.MustCompile(`[^a-z0-9]+`)
	anchor = reg.ReplaceAllString(anchor, "-")

	anchor = strings.Trim(anchor, "-")

	return anchor
}

// generateNavigation generates the sidebar navigation HTML
func generateNavigation(navItems []NavItem) string {
	var nav strings.Builder
	for _, item := range navItems {
		if item.Level <= 3 {
			nav.WriteString(`<li><a href="#`)
			nav.WriteString(item.Anchor)
			nav.WriteString(`" class="level-`)
			nav.WriteString(fmt.Sprintf("%d", item.Level))
			nav.WriteString(`">`)
			nav.WriteString(html.EscapeString(item.Title))
			nav.WriteString(`</a></li>`)
		}
	}
	return nav.String()
}
