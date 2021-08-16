/**
 * Copyright 2018 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// @export
export class Capability {
  static get(): Capability {
    if (Capability.instance === undefined) {
      Capability.instance = new Capability();
    }
    return Capability.instance;
  }

  private static instance: Capability;

  supported: boolean;
  indexedDb: boolean;
  webSql: boolean;
  localStorage: boolean;

  private agent: string;
  private browser: string;
  private version: number[];
  private versionMap: Map<string, string>;

  constructor() {
    this.supported = false;
    this.indexedDb = false;
    this.webSql = false;
    this.localStorage = false;
    this.agent = '';
    this.browser = '';
    this.version = [];
    this.versionMap = new Map<string, string>();

    this.detect();
  }

  getDetection(): string {
    return `${this.browser} ${this.version.join('.')}`;
  }

  private detect(): void {
    if (!this.detectNodeJS()) {
      if (navigator) {
        this.agent = navigator.userAgent;
        this.detectBrowser();
      }
    }
  }

  private convertVersion(version: string | undefined): void {
    if (version === undefined) {
      return;
    }

    this.version = version.split('.').map(v => {
      let n = 0;
      try {
        n = Number(v);
      } catch (e) {
        // Swallow error.
      }
      return n;
    });
  }

  private detectNodeJS(): boolean {
    if (typeof process !== typeof undefined && process.version) {
      // Assume this is NodeJS.
      this.convertVersion(process.version.slice(1));
      // Support only Node.js >= 7.0.
      this.supported = this.version[0] >= 7;
      this.browser = 'nodejs';
      this.indexedDb = false;
      this.webSql = false;
      return true;
    }
    return false;
  }

  private detectBrowser(): void {
    if (this.isIE() || this.isAndroid()) {
      return;
    }

    this.localStorage = true;
    this.detectVersion();
    const checkSequence = [
      this.isEdge.bind(this), // this must be placed before Chrome
      this.isFirefox.bind(this),
      this.isChrome.bind(this),
      this.isSafari.bind(this), // this must be placed after Chrome/Firefox
      this.isIOS.bind(this), // this must be placed after Safari
    ];

    checkSequence.some(fn => fn());
  }

  private detectVersion(): void {
    const regex = new RegExp(
      // Key. Note that a key may have a space.
      // (i.e. 'Mobile Safari' in 'Mobile Safari/5.0')
      '(\\w[\\w ]+)' +
        '/' + // slash
        '([^\\s]+)' + // version (i.e. '5.0b')
        '\\s*' + // whitespace
        '(?:\\((.*?)\\))?', // parenthetical info. parentheses not matched.
      'g'
    );

    let match: RegExpExecArray | null = null;
    do {
      match = regex.exec(this.agent);
      if (match) {
        const version = match[0] as string;
        this.versionMap.set(
          match[1] as string,
          version.slice(version.indexOf('/') + 1)
        );
      }
    } while (match);
  }

  private isChrome(): boolean {
    let detected = false;
    if (this.agent.indexOf('Chrome') !== -1) {
      detected = true;
      this.convertVersion(this.versionMap.get('Chrome'));
    } else if (this.agent.indexOf('CriOS') !== -1) {
      detected = true;
      this.convertVersion(this.versionMap.get('CriOS'));
    }

    if (detected) {
      this.browser = 'chrome';
      this.supported = this.version[0] > 60;
      this.indexedDb = true;
      this.webSql = true;
      this.localStorage = true;
      return true;
    }
    return false;
  }

  private isEdge(): boolean {
    if (this.agent.indexOf('Edge') !== -1) {
      this.browser = 'edge';
      this.supported = true;
      this.indexedDb = true;
      this.webSql = false;
      this.convertVersion(this.versionMap.get('Edge'));
      return true;
    }
    return false;
  }

  private isFirefox(): boolean {
    if (this.agent.indexOf('Firefox') !== -1) {
      this.browser = 'firefox';
      this.convertVersion('Firefox');
      this.supported = this.version[0] > 60;
      this.indexedDb = true;
      this.webSql = false;
      this.localStorage = true;
      return true;
    }
    return false;
  }

  private isIE(): boolean {
    if (
      this.agent.indexOf('Trident') !== -1 ||
      this.agent.indexOf('MSIE') !== -1
    ) {
      this.browser = 'ie';
      return true;
    }
    return false;
  }

  private isAndroid(): boolean {
    if (
      this.agent.indexOf('Android') !== -1 &&
      !this.isChrome() &&
      !this.isFirefox()
    ) {
      this.browser = 'legacy_android';
      return true;
    }
    return false;
  }

  private isSafari(): boolean {
    if (this.agent.indexOf('Safari') !== -1) {
      this.browser = 'safari';
      this.convertVersion(this.versionMap.get('Version'));
      this.supported = this.version[0] >= 10;
      this.indexedDb = this.supported;
      this.webSql = true;
      return true;
    }
    return false;
  }

  private isIOS(): boolean {
    if (
      this.agent.indexOf('AppleWebKit') !== -1 &&
      (this.agent.indexOf('iPad') !== -1 || this.agent.indexOf('iPhone') !== -1)
    ) {
      this.browser = 'ios_webview';
      this.convertVersion(this.versionMap.get('Version'));
      this.supported = this.version[0] >= 10;
      this.indexedDb = this.supported;
      this.webSql = true;
      return true;
    }
    return false;
  }
}
